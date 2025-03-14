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
import boto3
from boto3.s3.transfer import TransferConfig
from loguru import logger
from tqdm import tqdm

MB = 1024 * 1024
GB = 1024 * MB

logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> "
           "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
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
        logger.remove()
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
        s3.upload_file(
            Filename=file_path,
            Bucket=args.bucket,
            Key=args.key,
            Config=transfer_config,
            Callback=progress_callback
        )
        logger.success("Upload completed successfully.")
        return 0
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
