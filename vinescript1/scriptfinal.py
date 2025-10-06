import os
import time
import re
import json
import pandas as pd
import requests
import sys
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout, ConnectionError

# --- Configuration ---
INPUT_FILENAME = "archived_vine_links.parquet"
OUTPUT_FILENAME = "enriched_vine_data.parquet"
USER_AGENT = "MyVineMetadataValidator (data-project@example.com)"
BATCH_SIZE = 1000 # <-- Assuming you changed this to 1000
# WARNING: 0.006 seconds is extremely aggressive (166 req/sec) and may result in an IP ban.
REQUEST_DELAY_SECONDS = 0.05
REQUEST_TIMEOUT_SECONDS = 60
MAX_REQUEST_RETRIES = 3
# Set DEBUG_LIMIT_COUNT back to 0 to resume continuous scraping
DEBUG_LIMIT_COUNT = 0

# Regex to find the JSON data assigned to window.POST_DATA
# This looks for "window.POST_DATA = {..." followed by a closing semicolon and newline
POST_DATA_REGEX = re.compile(r"window\.POST_DATA\s*=\s*(\{.*?\})\s*;", re.DOTALL)

# --- Checkpoint Logic ---

def load_data():
    """Loads input links and determines which links are already processed."""
    if not os.path.exists(INPUT_FILENAME):
        print(f"Error: Input file '{INPUT_FILENAME}' not found. Exiting.")
        sys.exit(1)

    df_input = pd.read_parquet(INPUT_FILENAME, engine='fastparquet')

    # Ensure the input DataFrame has no duplicate source URLs
    df_input = df_input.drop_duplicates(subset=['original_url']).reset_index(drop=True)

    # Define columns for the enriched DataFrame, including the new metrics
    output_cols = ['original_url', 'wayback_url', 'status', 'video_url_cdn', 'title', 'user_id', 'loops', 'likes', 'revines', 'comments']

    # Check if the output file exists to handle resumption
    if os.path.exists(OUTPUT_FILENAME):
        df_output = pd.read_parquet(OUTPUT_FILENAME, engine='fastparquet')
        processed_urls = set(df_output['original_url'])
        total_processed = len(df_output)

        print(f"Resuming enrichment from existing output file: {OUTPUT_FILENAME}")
        print(f"Found {total_processed:,} records already processed.")

        # Filter input DataFrame to only include unprocessed URLs
        df_input = df_input[~df_input['original_url'].isin(processed_urls)].reset_index(drop=True)
    else:
        df_output = pd.DataFrame(columns=output_cols)
        total_processed = 0
        print("Starting enrichment from scratch (no output file found).")

    return df_input, df_output, total_processed

def save_data(new_data, current_df):
    """Appends new data and saves it to the output Parquet file."""

    new_df = pd.DataFrame(new_data)

    # Append the new batch to the existing processed data
    updated_df = pd.concat([current_df, new_df], ignore_index=True)

    # Save the combined DataFrame to Parquet
    updated_df.to_parquet(OUTPUT_FILENAME, engine='fastparquet', index=False)

    return updated_df

# --- Parsing Logic (FINAL VERSION) ---

def get_metadata(html: str) -> dict:
    """
    Extracts core metrics from the window.POST_DATA JavaScript object found in the HTML.
    """
    metrics = {
        'user_id': pd.NA,
        'video_url_cdn': pd.NA,
        'title': pd.NA,
        'loops': pd.NA,
        'likes': pd.NA,
        'revines': pd.NA,
        'comments': pd.NA,
    }

    # 1. Use Regex to extract the raw JSON string
    match = POST_DATA_REGEX.search(html)
    if not match:
        return metrics # Failed to find the data block

    json_str = match.group(1)

    # 2. Extract core video ID to find the correct key in the POST_DATA dictionary
    # The VINE ID is inside the JSON as the top-level key (e.g., "5000ddrT1MP")
    try:
        # Load the JSON object
        post_data = json.loads(json_str)

        # The key is the short Vine ID; we assume only one post is on the page.
        vine_id = next(iter(post_data.keys()), None)
        if not vine_id:
            return metrics

        post = post_data[vine_id]

        # 3. Extract requested metrics using specific JSON paths
        # Note: Reposts are used for Revines
        metrics['loops'] = post.get('loops', {}).get('count', pd.NA)
        metrics['likes'] = post.get('likes', {}).get('count', pd.NA)
        metrics['revines'] = post.get('reposts', {}).get('count', pd.NA)
        metrics['comments'] = post.get('comments', {}).get('count', pd.NA)

        metrics['user_id'] = post.get('userIdStr', pd.NA)

        # 4. Extract core video information (also inside POST_DATA or related tags)
        # We can still check meta tags for the main video URL as a backup for metrics we missed
        soup = BeautifulSoup(html, "html.parser")
        for el in soup.select("head meta"):
            prop = el.attrs.get("property") or el.attrs.get("name")
            content = el.attrs.get("content")
            if prop == 'twitter:player:stream':
                metrics['video_url_cdn'] = content
            elif prop == 'twitter:title':
                metrics['title'] = content

    except json.JSONDecodeError:
        # Handle cases where the JSON structure is malformed
        return metrics
    except Exception:
        # Handle general unexpected errors
        return metrics

    return metrics

# --- Main Logic ---

def format_timedelta(seconds):
    """Formats seconds into HH:MM:SS string."""
    if seconds <= 0:
        return "00:00:00"
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

def fetch_and_enrich(row: pd.Series, session: requests.Session) -> dict:
    """
    Fetches the archived page, retries on transient failures, and extracts metadata.
    """
    headers = {'User-Agent': USER_AGENT}
    playback_url = row['wayback_url']

    for attempt in range(MAX_REQUEST_RETRIES):
        try:
            response = session.get(playback_url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()

            html_content = response.content.decode('utf-8', errors='ignore')

            # Since DEBUG_LIMIT_COUNT is now 0, this block won't run in continuous mode
            if DEBUG_LIMIT_COUNT > 0:
                print("\n\n" + "="*50)
                print(f"RAW HTML CONTENT RECEIVED FOR URL: {playback_url}")
                print("="*50)
                print(html_content)
                print("="*50 + "\n")

            metrics = get_metadata(html_content)

            # Final output construction
            result = {
                'original_url': row['original_url'],
                'wayback_url': row['wayback_url'],
                'status': 'VIDEO_METADATA_FOUND' if pd.notna(metrics['video_url_cdn']) else 'NO_METADATA_FOUND',
                'video_url_cdn': metrics.get('video_url_cdn'),
                'title': metrics.get('title'),
                'user_id': metrics.get('user_id'),
                'loops': metrics.get('loops'),
                'likes': metrics.get('likes'),
                'revines': metrics.get('revines'),
                'comments': metrics.get('comments'),
            }

            return result

        except (Timeout, ConnectionError) as e:
            if attempt < MAX_REQUEST_RETRIES - 1:
                time.sleep(REQUEST_DELAY_SECONDS * 3) # Wait longer on error
                continue
            return {'original_url': row['original_url'], 'wayback_url': row['wayback_url'], 'status': 'HTTP_ERROR', 'video_url_cdn': pd.NA, 'title': pd.NA, 'user_id': pd.NA, 'loops': pd.NA, 'likes': pd.NA, 'revines': pd.NA, 'comments': pd.NA}

        except RequestException as e:
            status_code = e.response.status_code if e.response is not None else 'Unknown'
            status_map = {429: '429_RATE_LIMITED', 404: '404_ARCHIVE_MISSING'}
            status = status_map.get(status_code, f'HTTP_ERROR_{status_code}')

            return {'original_url': row['original_url'], 'wayback_url': row['wayback_url'], 'status': status, 'video_url_cdn': pd.NA, 'title': pd.NA, 'user_id': pd.NA, 'loops': pd.NA, 'likes': pd.NA, 'revines': pd.NA, 'comments': pd.NA}


def run_enrichment():
    df_input, df_output, total_processed = load_data()

    total_new_links = len(df_input)
    if total_new_links == 0:
        print("\nAll available links have already been processed.")
        return

    total_target_count = total_processed + total_new_links
    print(f"\nStarting enrichment for {total_new_links:,} new links. Total target: {total_target_count:,}")

    session = requests.Session()
    new_data_batch = []

    try:
        start_time = time.time()
        last_save_time = start_time

        # --- FIX: Initialize the total count correctly ---
        current_processed_count = total_processed
        # ------------------------------------------------

        # --- Adjusted loop to run continuously (DEBUG_LIMIT_COUNT is ignored when set to 0) ---
        for i, row in df_input.iterrows():
            if DEBUG_LIMIT_COUNT > 0 and i >= DEBUG_LIMIT_COUNT:
                break
        # --- END Adjusted loop ---

            metrics = fetch_and_enrich(row, session)
            new_data_batch.append(metrics)

            # --- FIX: Update the count logic ---
            current_processed_count += 1

            # --- TIME REMAINING CALCULATION ---
            time_elapsed = time.time() - start_time
            items_remaining = total_target_count - current_processed_count

            # Recalculate average speed based on total processed items
            avg_speed = (current_processed_count - total_processed) / time_elapsed if time_elapsed > 0 else 0

            # Recalculate ETA
            time_remaining_seconds = items_remaining / avg_speed if avg_speed > 0 else 0

            # Update status line
            sys.stdout.write(
                f"\rProcessing total entries found: {current_processed_count:,} | Rate: {avg_speed:.2f} items/sec | ETA: {format_timedelta(time_remaining_seconds)}"
            )
            sys.stdout.flush()

            # Checkpoint Logic: Trigger when the *new batch* size reaches the BATCH_SIZE
            if len(new_data_batch) >= BATCH_SIZE:
                df_output = save_data(new_data_batch, df_output)

                # --- FIX: Update total_processed after saving ---
                total_processed = len(df_output)
                current_processed_count = total_processed
                # ------------------------------------------------

                new_data_batch = []

                # Report elapsed time since last save
                time_since_save = time.time() - last_save_time
                print(f" | Checkpoint saved in {time_since_save:.2f} seconds.")
                last_save_time = time.time()

            # Be polite (or aggressive, as configured)
            time.sleep(REQUEST_DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n\nUser interrupted process. Saving final batch...")

    # Final save of any remaining data in the last batch
    if new_data_batch:
        df_output = save_data(new_data_batch, df_output)

    session.close()

    print("\n\n--- Enrichment Complete (or stopped) ---")
    print(f"Total unique records saved to '{OUTPUT_FILENAME}': {len(df_output):,}")

if __name__ == '__main__':
    run_enrichment()
