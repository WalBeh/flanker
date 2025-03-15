#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "loguru",
#     "boto3",
#     "tqdm",
# ]
# ///
# tool that uses the AWS SDK to upload a file to S3, supporting files >5GB

"""
Usage:
  ./flanker.py /path/to/file --bucket my-bucket --key "folder/my-file" [--region us-east-1]

This script uses local AWS credentials (e.g., environment variables, config files, or IAM role)
to upload a file to S3 with multi-part support. Large files (>5GB) will be automatically split into
parts and uploaded in parallel. A progress bar is shown using tqdm.
"""

import argparse
import os
import sys
import signal
import datetime
import boto3
import hashlib
from boto3.s3.transfer import TransferConfig
from loguru import logger
from tqdm import tqdm

MB = 1024 * 1024
GB = 1024 * MB

logger.remove()
logger.add(
    "flanker.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG"
)

class ProgressPercentage:
    """
    Callback for boto3's s3 upload operations to show progress with tqdm.
    """
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._pbar = tqdm(
            total=self._size,
            unit="B",
            unit_scale=True,
            desc="Uploading"
        )
        self._seen_so_far = 0

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        self._pbar.update(bytes_amount)
        if self._seen_so_far >= self._size:
            self._pbar.close()

def verify_upload_integrity(file_path, etag):
    """Verify the integrity of an uploaded file by comparing its MD5 hash with the S3 ETag."""
    # Strip quotes from ETag if present
    etag = etag.strip('"')

    # Check if it's a multipart ETag (contains a dash)
    if '-' in etag:
        logger.info("Multipart upload detected. Simple MD5 verification not possible.")
        return None

    # Calculate MD5 hash of the local file
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            md5_hash.update(chunk)

    local_md5 = md5_hash.hexdigest()

    # Compare hashes
    if local_md5 == etag:
        return True
    else:
        return False

def parse_arguments():
    parser = argparse.ArgumentParser(description="Upload a file to S3 using boto3 with multi-part support (>5GB).")
    parser.add_argument("file_path", help="Local path to the file to upload.")
    parser.add_argument("--bucket", default="cratedb-cloud-heapdumps", help="Target S3 bucket name.")
    parser.add_argument("--key", required=True,  help="Object key (path) in the S3 bucket.")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1).")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging.")
    return parser.parse_args()

def handle_sigint(sig, _):
    print()
    logger.info("Upload interrupted by user (SIGINT). Exiting gracefully.")
    sys.exit(130)

def main():
    args = parse_arguments()

    if args.verbose:
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> "
                   "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="DEBUG"
        )
        logger.debug("Verbose logging enabled")

    signal.signal(signal.SIGINT, handle_sigint)

    file_path = args.file_path
    if not os.path.isfile(file_path):
        logger.error(f"File not found: {file_path}")
        return 1

    file_size = os.path.getsize(file_path)
    file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
    size_str = f"{file_size / MB:.2f} MB" if file_size < GB else f"{file_size / GB:.2f} GB"
    logger.info(f"File: {os.path.basename(file_path)}, Size: {size_str}, Modified: {file_mtime.strftime('%Y-%m-%d %H:%M:%S')}")

    s3 = boto3.client("s3", region_name=args.region)

    # Configure multi-part threshold to 5GB (anything bigger automatically uses multi-part).
    transfer_config = TransferConfig(
        multipart_threshold=5 * GB,
        max_concurrency=4,
        multipart_chunksize=64 * MB,
        use_threads=True
    )

    logger.info(f"Uploading {file_path} to s3://{args.bucket}/{args.key} ...")

    # The callback handles the tqdm progress
    progress_callback = ProgressPercentage(file_path)

    # Run the upload
    try:
        start_time = datetime.datetime.now()
        s3.upload_file(
            Filename=file_path,
            Bucket=args.bucket,
            Key=args.key,
            Config=transfer_config,
            Callback=progress_callback
        )
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        elapsed_seconds = elapsed_time.total_seconds()

        # Get object metadata to confirm upload
        object_info = s3.head_object(Bucket=args.bucket, Key=args.key)

        # Calculate and display statistics
        file_size_bytes = os.path.getsize(file_path)
        upload_speed_mbps = (file_size_bytes / (1024 * 1024)) / elapsed_seconds if elapsed_seconds > 0 else 0

        logger.success(f"Upload completed successfully in {elapsed_time}")
        logger.info(f"Average upload speed: {upload_speed_mbps:.2f} MB/s")
        logger.info(f"S3 object: s3://{args.bucket}/{args.key}")
        logger.info(f"ETag: {object_info.get('ETag', 'N/A')}")
        logger.info(f"Storage class: {object_info.get('StorageClass', 'STANDARD')}")

        etag = object_info.get('ETag', 'N/A').strip('"')
        logger.info(f"ETag: \"{etag}\"")

        # Verify integrity if not a multipart upload
        if '-' not in etag:
            is_valid = verify_upload_integrity(file_path, etag)
            if is_valid:
                logger.success("File integrity verified: Local MD5 matches S3 ETag")
            else:
                logger.warning("File integrity check failed: Local MD5 doesn't match S3 ETag")
        else:
            logger.warning("Multipart upload detected - skipping simple integrity check")

        return 0

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
