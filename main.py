# -*- coding: utf-8 -*-
"""
Downloads recordings and metadata from the Xeno-Canto API and uploads them to S3,
now with a delightful progress bar.
"""

import argparse
import json
import os
import time
import requests
import boto3 # type: ignore
from botocore.exceptions import NoCredentialsError, PartialCredentialsError # type: ignore
from typing import Dict, Any, List, Optional
from tqdm import tqdm # Import tqdm

# --- Constants ---

API_ENDPOINT: str = "https://xeno-canto.org/api/2/recordings"
S3_BUCKET: str = "alexdong-bioacoustics"
S3_PREFIX: str = "xeno-canto/"
REQUEST_DELAY_SECONDS: float = 1.1 # Slightly more than 1 to be safe

# --- Core Functions ---

def upload_to_s3(
    s3_client: Any, # boto3 S3 client is not easily typed without mypy stubs
    bucket: str,
    s3_key: str,
    data: bytes,
    content_type: str | None = None,
    quiet: bool = False, # Added flag to suppress print for progress bar
) -> None:
    """Uploads data (bytes) to a specific S3 key."""
    if not quiet:
        print(f"[INFO] Uploading to s3://{bucket}/{s3_key}...")
    try:
        extra_args = {}
        if content_type:
            extra_args['ContentType'] = content_type

        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=data, **extra_args)
        if not quiet:
            print(f"[SUCCESS] Successfully uploaded to s3://{bucket}/{s3_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        # Use tqdm.write if available to avoid breaking bar, otherwise print
        log_func = tqdm.write if tqdm else print
        log_func(f"[ERROR] S3 credentials not found or incomplete: {e}")
        raise
    except Exception as e:
        log_func = tqdm.write if tqdm else print
        log_func(f"[ERROR] Failed to upload {s3_key} to S3: {e}")
        raise


def check_s3_file_exists(
    s3_client: Any,
    bucket: str,
    s3_key: str,
) -> bool:
    """Checks if a file exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except Exception:
        return False


def download_and_upload_recording(
    recording: Dict[str, Any],
    s3_client: Any,
    bucket: str,
    prefix: str,
    pbar: Optional[tqdm] = None, # Pass the progress bar instance
) -> None:
    """Downloads a single audio file and its metadata, then uploads both to S3."""
    recording_id: str = recording.get("id")
    assert recording_id, "Recording data must contain an 'id'."
    # Use tqdm.write for logging to avoid messing up the progress bar
    log_func = pbar.write if pbar else print

    # 1. Prepare JSON metadata path
    json_s3_key = f"{prefix}{recording_id}.json"
    
    # 2. Prepare Audio File path
    file_url_relative: str | None = recording.get("file")
    original_filename: str | None = recording.get("file-name")

    if not file_url_relative or not original_filename:
        return

    # Prepare audio S3 key
    audio_s3_key = f"{prefix}{original_filename}"
    if not prefix.endswith('/') and prefix: # Ensure slash if prefix exists
        audio_s3_key = f"{prefix}/{original_filename}"
    elif not prefix: # Handle empty prefix
         audio_s3_key = original_filename
    
    # Check if both files already exist in S3
    json_exists = check_s3_file_exists(s3_client, bucket, json_s3_key)
    audio_exists = check_s3_file_exists(s3_client, bucket, audio_s3_key)
    
    if json_exists and audio_exists:
        log_func(f"[INFO] Skipping {recording_id} - already exists in S3")
        if pbar:
            pbar.update(1)
        return

    # Upload JSON metadata if it doesn't exist
    if not json_exists:
        json_data = json.dumps(recording, indent=4).encode("utf-8")
        upload_to_s3(s3_client, bucket, json_s3_key, json_data, "application/json", quiet=bool(pbar))

    # Download and upload audio file if it doesn't exist
    if not audio_exists:
        # Handle protocol-relative URL (starts with //)
        if file_url_relative.startswith("//"):
            file_url = f"https:{file_url_relative}"
        else:
            file_url = file_url_relative
            if not file_url.startswith("http"):
                 log_func(f"[WARNING] Unexpected file URL format for {recording_id}: {file_url}. Prepending https:")
                 if not file_url.startswith('/'):
                     file_url = '/' + file_url
                 file_url = f"https://xeno-canto.org{file_url}"

        try:
            time.sleep(REQUEST_DELAY_SECONDS) # Delay *before* download request
            response = requests.get(file_url, stream=True, timeout=60)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            log_func(f"[ERROR] Failed to download audio for {recording_id}: {e}")
            raise

        try:
            content_type = response.headers.get('Content-Type', 'application/octet-stream')
            s3_client.upload_fileobj(
                response.raw,
                bucket,
                audio_s3_key,
                ExtraArgs={'ContentType': content_type}
            )
        except (NoCredentialsError, PartialCredentialsError) as e:
            log_func(f"[ERROR] S3 credentials not found or incomplete during audio upload for {recording_id}: {e}")
            raise
        except Exception as e:
            log_func(f"[ERROR] Failed to upload audio for {recording_id} to S3: {e}")
            raise
        finally:
            response.close()

    # Update Progress Bar (after successful completion of both uploads for this recording)
    if pbar:
        pbar.update(1)


def fetch_and_process_pages(query: str, s3_client: Any, bucket: str, prefix: str, start_page: int = 1) -> None:
    """Fetches all pages for a query and processes each recording, showing progress."""
    current_page: int = start_page
    total_pages: int = 1 # Assume 1 initially
    num_recordings_total_str: str = "0"
    processed_recordings_count: int = 0
    recording_pbar: Optional[tqdm] = None

    print(f"[INFO] Fetching initial metadata to determine total recordings...")

    try:
        # First, get the total number of pages and recordings
        api_url = f"{API_ENDPOINT}?query={query}&page=1"
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data: Dict[str, Any] = response.json()
            
            total_pages = int(data.get("numPages", 1))
            num_recordings_total_str = data.get("numRecordings", "0")
            num_recordings_total = int(num_recordings_total_str)
            
            print(f"[INFO] Query found {num_recordings_total_str} recordings across {total_pages} pages.")
            
            if num_recordings_total == 0:
                print("[INFO] No recordings found for the query. Exiting.")
                return
                
            # Calculate how many recordings we're skipping
            if start_page > 1:
                recordings_per_page = len(data.get("recordings", []))
                skipped_recordings = (start_page - 1) * recordings_per_page
                print(f"[INFO] Starting from page {start_page}, skipping approximately {skipped_recordings} recordings.")
                # Adjust total for progress bar
                num_recordings_total = max(0, num_recordings_total - skipped_recordings)
            
            # Initialize the progress bar here
            recording_pbar = tqdm(
                total=num_recordings_total,
                desc="Processing Recordings",
                unit="file",
                ncols=100
            )
            
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Failed to fetch API data for initial page: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to decode JSON response for initial page: {e}")
            raise

        # Now process from the requested start page
        while current_page <= total_pages:
            api_url = f"{API_ENDPOINT}?query={query}&page={current_page}"
            log_func = recording_pbar.write if recording_pbar else print
            log_func(f"[INFO] Fetching page {current_page} / {total_pages}...")

            try:
                time.sleep(REQUEST_DELAY_SECONDS) # Delay *before* metadata page request
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
                data: Dict[str, Any] = response.json()

            except requests.exceptions.RequestException as e:
                log_func(f"[ERROR] Failed to fetch API data for page {current_page}: {e}")
                raise
            except json.JSONDecodeError as e:
                log_func(f"[ERROR] Failed to decode JSON response for page {current_page}: {e}")
                log_func(f"[DEBUG] Response text: {response.text[:500]}...")
                raise

            if "error" in data:
                error_info = data["error"]
                log_func(f"[ERROR] API returned error: {error_info.get('code')} - {error_info.get('message')}")
                raise ValueError(f"API Error: {error_info.get('code')} - {error_info.get('message')}")

            recordings: List[Dict[str, Any]] = data.get("recordings", [])

            for recording in recordings:
                # Pass the progress bar instance to the download function
                download_and_upload_recording(recording, s3_client, bucket, prefix, recording_pbar)
                processed_recordings_count +=1

            current_page += 1

    finally:
        # Ensure the progress bar is closed cleanly, even if errors occurred
        if recording_pbar:
            # If processing stopped early, reflect the actual count
            recording_pbar.n = processed_recordings_count
            recording_pbar.refresh() # Update display
            recording_pbar.close()
            print(f"[INFO] Processed {processed_recordings_count} of {num_recordings_total_str} recordings.")


# --- Main Execution ---

def main() -> None:
    """Parses arguments and initiates the download and upload process."""
    parser = argparse.ArgumentParser(
        description="Download Xeno-Canto recordings and metadata to S3."
    )
    parser.add_argument(
        "-q",
        "--query",
        required=True,
        help="The Xeno-Canto search query (e.g., 'cnt:brazil', 'troglodytes+troglodytes')",
    )
    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help=f"The S3 bucket name (default: {S3_BUCKET})",
    )
    parser.add_argument(
        "--prefix",
        default=S3_PREFIX,
        help=f"The S3 prefix (folder path) within the bucket (default: {S3_PREFIX})",
    )
    parser.add_argument(
        "-p",
        "--start-page",
        type=int,
        default=1,
        help="Page number to start from (default: 1)",
    )
    args = parser.parse_args()

    s3_prefix = args.prefix
    if not s3_prefix.endswith("/") and s3_prefix:
        s3_prefix += "/"
    elif not s3_prefix:
        s3_prefix = ""

    print(f"🚀 Starting Xeno-Canto download process!")
    print(f"🔍 Query: {args.query}")
    print(f"☁️  Target S3 Bucket: {args.bucket}")
    print(f"📁 Target S3 Prefix: {s3_prefix}")
    print(f"📄 Starting from page: {args.start_page}")
    print(f"⏱️  API Delay: {REQUEST_DELAY_SECONDS} seconds between requests")

    try:
        s3_client = boto3.client("s3")
        s3_client.list_buckets() # Basic check
        print("[INFO] S3 client initialized and credentials seem valid.")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"[FATAL] S3 credentials error on initialization: {e}. "
              "Ensure AWS credentials are configured.")
        return
    except Exception as e:
        print(f"[FATAL] Could not initialize S3 client: {e}")
        raise

    fetch_and_process_pages(args.query, s3_client, args.bucket, s3_prefix, args.start_page)

    print("✅ Download and upload process finished!")


if __name__ == "__main__":
    main()
