#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "boto3",
#     "loguru",
#     "requests",
#     "tqdm",
# ]
# ///
# tool that uses the aws sdk to upload a file to an s3 bucket using pre-signed urls

"""
Tool that uses the AWS SDK to upload a file to an S3 bucket using pre-signed URLs.
The file to upload and the pre-signed URL are provided as command-line arguments.

Flanker (rugby):
Openside flankers need to be quick, agile, and possess exceptional ball-hunting skills.
Their ability to read the game, anticipate breakdown situations, and apply pressure on
the opposition makes them a valuable asset in both attack and defense.
"""

import argparse
import datetime
import os
import sys
import urllib.parse
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
from botocore.exceptions import ClientError
from loguru import logger
from tqdm import tqdm

# Configure Loguru
timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
#log_file = f"flanker-{timestamp}.log"
log_file = f"flanker.log"

logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    log_file,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="DEBUG",  # Log everything to file
    rotation="20 MB",  # Rotate when file reaches 20MB
    retention=5  # Keep 5 log files
)

logger.info(f"Started logging to {log_file}")


class S3Uploader:
    def __init__(self, part_size_mb=10, max_workers=10):
        """
        Initialize the S3 uploader.

        Args:
            part_size_mb (int): Size of each part for multipart upload in MB
            max_workers (int): Maximum number of concurrent upload threads
        """
        self.part_size_bytes = part_size_mb * 1024 * 1024
        self.max_workers = max_workers

    def upload_with_presigned_url(self, file_path, presigned_url):
        """
        Upload a file to S3 using a pre-signed URL.

        Args:
            file_path (str): Path to the file to upload
            presigned_url (str): Pre-signed URL for the S3 upload

        Returns:
            bool: True if upload was successful, False otherwise
        """
        # Extract bucket and key from presigned URL
        parsed_url = urllib.parse.urlparse(presigned_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Check if this is a multipart upload URL or direct upload URL
        if "partNumber" in query_params:
            logger.info("Detected multipart upload pre-signed URL")
            return self._handle_multipart_url(file_path, presigned_url)
        else:
            logger.info("Using direct upload with pre-signed URL")
            return self._direct_upload(file_path, presigned_url)

    def _direct_upload(self, file_path, presigned_url):
        """
        Upload a file directly using a pre-signed URL, using boto3 for better compatibility.

        Args:
            file_path (str): Path to the file to upload
            presigned_url (str): Pre-signed URL for the S3 upload

        Returns:
            bool: True if upload was successful, False otherwise
        """
        file_size = os.path.getsize(file_path)
        logger.info(f"Uploading file {file_path} ({file_size / (1024*1024):.2f} MB)")

        # Parse the presigned URL to get bucket and key
        parsed_url = urllib.parse.urlparse(presigned_url)
        bucket = parsed_url.netloc.split('.')[0]  # Extract bucket from hostname
        key = parsed_url.path.lstrip('/')

        # Parse query parameters to extract authentication details
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Create a boto3 session with the specific credentials from the URL
        try:
            # Try a direct upload with the standard boto3 client first
            logger.debug("Attempting direct upload with boto3 S3 client")

            # Create a boto3 session with no specific credentials (will use environment/profile)
            session = boto3.Session()
            s3_client = session.client('s3')

            # Upload the file with progress tracking
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading with boto3") as pbar:
                s3_client.upload_file(
                    file_path,
                    bucket,
                    key,
                    Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
                    ExtraArgs={
                        # Add any headers that might be needed for the presigned URL
                        'ContentType': 'application/octet-stream',
                    }
                )

            logger.success(f"Successfully uploaded {file_path} to {bucket}/{key}")
            return True

        except ClientError as e:
            logger.warning(f"Direct boto3 upload failed: {e}")
            logger.warning("Attempting to use the presigned URL with boto3's low-level API")

            # Extract endpoint from the URL
            endpoint_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

            try:
                # Create a custom boto3 session with an explicit endpoint
                session = boto3.Session()
                s3_client = session.client(
                    's3',
                    endpoint_url=endpoint_url,
                    config=boto3.session.Config(
                        signature_version=boto3.UNSIGNED,  # Don't sign requests, URL is already signed
                        s3={'addressing_style': 'virtual'}  # Use virtual-hosted style
                    )
                )

                # Open the file and upload in chunks with progress tracking
                total_chunks = (file_size + self.part_size_bytes - 1) // self.part_size_bytes

                with open(file_path, 'rb') as file_obj:
                    with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading in chunks") as pbar:
                        for chunk_idx in range(total_chunks):
                            # Read a chunk
                            start_pos = chunk_idx * self.part_size_bytes
                            file_obj.seek(start_pos)
                            data = file_obj.read(min(self.part_size_bytes, file_size - start_pos))

                            # Create a custom request dict that uses the presigned URL directly
                            request_dict = {
                                'url': presigned_url,
                                'body': data,
                                'headers': {'Content-Type': 'application/octet-stream'},
                                'context': {'client_region': 'us-east-1'},  # Default region
                            }

                            # Make a low-level request
                            http = s3_client._endpoint.http_session
                            http.request('PUT', presigned_url, data=data, headers={'Content-Type': 'application/octet-stream'})

                            # Update progress
                            pbar.update(len(data))

                logger.success(f"Successfully uploaded {file_path} using presigned URL")
                return True

            except Exception as e2:
                logger.error(f"Both upload methods failed. Last error: {e2}")
                logger.error("Falling back to using Python requests with custom settings")

                # Final attempt with custom requests settings
                try:
                    # Create a session with custom settings
                    session = requests.Session()

                    # Use a custom adapter with longer timeouts
                    adapter = requests.adapters.HTTPAdapter(
                        pool_connections=1,
                        pool_maxsize=1,
                        max_retries=requests.adapters.Retry(
                            total=5,
                            backoff_factor=0.5,
                            status_forcelist=[500, 502, 503, 504]
                        )
                    )
                    session.mount('https://', adapter)

                    # Add option to parse arguments
                    parser = argparse.ArgumentParser()
                    parser.add_argument('--no-verify-ssl', action='store_true')
                    args, _ = parser.parse_known_args()

                    # Set verify based on args
                    verify = not args.no_verify_ssl if hasattr(args, 'no_verify_ssl') else True

                    if not verify:
                        logger.warning("SSL verification disabled")
                        import urllib3
                        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

                    with open(file_path, 'rb') as file_data:
                        headers = {'Content-Type': 'application/octet-stream'}
                        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading with requests") as pbar:
                            response = session.put(
                                presigned_url,
                                data=TqdmFileReader(file_data, pbar),
                                headers=headers,
                                timeout=600,  # 10 minute timeout
                                verify=verify
                            )
                        response.raise_for_status()

                    logger.success(f"Upload completed successfully with requests: {response.status_code}")
                    return True

                except requests.exceptions.RequestException as e3:
                    logger.error(f"All upload methods failed. Last error: {e3}")
                    return False

    def _handle_multipart_url(self, file_path, presigned_url):
        """
        Handle multipart upload URL. This is a placeholder since a single pre-signed URL
        can't be used for multipart uploads (each part needs its own URL).

        Args:
            file_path (str): Path to the file to upload
            presigned_url (str): Pre-signed URL for the S3 upload

        Returns:
            bool: True if upload was successful, False otherwise
        """
        logger.error("A single pre-signed URL can't be used for multipart uploads.")
        logger.error("Each part requires its own pre-signed URL. Please provide:")
        logger.error("1. A simple pre-signed URL for direct upload")
        logger.error("2. Or multiple pre-signed URLs (one per part)")
        logger.error("3. Or AWS credentials to generate the URLs for each part")

        return False

    def upload_with_aws_credentials(self, file_path, bucket, key, profile=None, region=None):
        """
        Upload a file using AWS credentials and multipart upload if the file is large.

        Args:
            file_path (str): Path to the file to upload
            bucket (str): S3 bucket name
            key (str): S3 object key
            profile (str): AWS profile name (optional)
            region (str): AWS region (optional)

        Returns:
            bool: True if upload was successful, False otherwise
        """
        # Create a session using the provided profile
        session = boto3.Session(profile_name=profile, region_name=region)
        s3_client = session.client('s3')

        file_size = os.path.getsize(file_path)

        try:
            if file_size > self.part_size_bytes:
                return self._multipart_upload(s3_client, file_path, bucket, key)
            else:
                logger.info(f"File size {file_size} bytes is small enough for direct upload")
                return self._upload_file(s3_client, file_path, bucket, key)
        except Exception as e:
            logger.error(f"Error during upload: {str(e)}")
            return False

    def _upload_file(self, s3_client, file_path, bucket, key):
        """
        Upload a file directly to S3.

        Args:
            s3_client: boto3 S3 client
            file_path (str): Path to the file to upload
            bucket (str): S3 bucket name
            key (str): S3 object key

        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            file_size = os.path.getsize(file_path)
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading") as pbar:
                s3_client.upload_file(
                    file_path,
                    bucket,
                    key,
                    Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
                )

            logger.success(f"Successfully uploaded {file_path} to {bucket}/{key}")
            return True

        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            return False

    def _multipart_upload(self, s3_client, file_path, bucket, key):
        """
        Perform multipart upload for large files.

        Args:
            s3_client: boto3 S3 client
            file_path (str): Path to the file to upload
            bucket (str): S3 bucket name
            key (str): S3 object key

        Returns:
            bool: True if upload was successful, False otherwise
        """
        file_size = os.path.getsize(file_path)
        logger.info(f"Starting multipart upload for {file_path} ({file_size / (1024*1024):.2f} MB)")

        # Initiate the multipart upload
        try:
            mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = mpu["UploadId"]
            logger.info(f"Multipart upload initiated with ID: {upload_id}")

            # Calculate the number of parts
            parts_count = (file_size + self.part_size_bytes - 1) // self.part_size_bytes
            logger.info(f"Uploading {parts_count} parts of {self.part_size_bytes/(1024*1024):.2f} MB each")

            # Prepare parts information
            parts_info = []

            # Upload each part in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []

                with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading") as pbar:
                    for part_number in range(1, parts_count + 1):
                        # Calculate byte range for this part
                        start_byte = (part_number - 1) * self.part_size_bytes
                        end_byte = min(part_number * self.part_size_bytes, file_size) - 1
                        part_size = end_byte - start_byte + 1

                        # Submit upload task to thread pool
                        futures.append(
                            executor.submit(
                                self._upload_part,
                                s3_client,
                                bucket,
                                key,
                                upload_id,
                                file_path,
                                part_number,
                                start_byte,
                                part_size,
                                pbar
                            )
                        )

                # Process results as they complete
                for future in as_completed(futures):
                    result = future.result()
                    if result is None:
                        # Upload part failed, abort the multipart upload
                        logger.error("Part upload failed, aborting multipart upload")
                        s3_client.abort_multipart_upload(
                            Bucket=bucket, Key=key, UploadId=upload_id
                        )
                        return False
                    parts_info.append(result)

            # Complete the multipart upload
            parts_info.sort(key=lambda x: x["PartNumber"])  # Sort parts by part number

            logger.info(f"All {len(parts_info)} parts uploaded successfully, completing multipart upload")
            s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts_info}
            )

            logger.success(f"Multipart upload completed for {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error in multipart upload: {str(e)}")
            # Attempt to abort the multipart upload if it was created
            if 'upload_id' in locals():
                try:
                    s3_client.abort_multipart_upload(
                        Bucket=bucket, Key=key, UploadId=upload_id
                    )
                    logger.info("Aborted incomplete multipart upload")
                except Exception as abort_error:
                    logger.error(f"Error aborting multipart upload: {str(abort_error)}")
            return False

    def _upload_part(self, s3_client, bucket, key, upload_id, file_path,
                     part_number, start_byte, part_size, pbar):
        """
        Upload a single part of a multipart upload.

        Args:
            s3_client: boto3 S3 client
            bucket (str): S3 bucket name
            key (str): S3 object key
            upload_id (str): Multipart upload ID
            file_path (str): Path to the file being uploaded
            part_number (int): Part number
            start_byte (int): Starting byte of this part
            part_size (int): Size of this part in bytes
            pbar: tqdm progress bar

        Returns:
            dict: Part information on success, None on failure
        """
        try:
            with open(file_path, 'rb') as f:
                f.seek(start_byte)
                part_data = f.read(part_size)

            # Upload the part
            response = s3_client.upload_part(
                Body=part_data,
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id
            )

            # Update progress bar
            pbar.update(part_size)

            return {
                "PartNumber": part_number,
                "ETag": response["ETag"]
            }

        except Exception as e:
            logger.error(f"Error uploading part {part_number}: {str(e)}")
            return None


class TqdmFileReader:
    """Wrap a file object to update tqdm progress bar when read."""
    def __init__(self, file_obj, pbar):
        self.file_obj = file_obj
        self.pbar = pbar

    def read(self, size=-1):
        data = self.file_obj.read(size)
        self.pbar.update(len(data))
        return data


# Add this function to handle clean exits
def handle_sigint(signal, frame):
    """Handle keyboard interrupt gracefully."""
    print()  # Add a newline after ^C
    logger.info("Upload interrupted by user. Cleaning up...")
    logger.info("Thank you for using Flanker. The upload was not completed.")
    sys.exit(130)  # Standard exit code for SIGINT


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Upload a file to S3 using a pre-signed URL or AWS credentials'
    )

    parser.add_argument('file_path', help='Path to the file to upload')

    # Pre-signed URL method
    parser.add_argument('--url', help='Pre-signed URL for the upload')

    # AWS credentials method
    parser.add_argument('--bucket', help='S3 bucket name (when using AWS credentials)')
    parser.add_argument('--key', help='S3 object key (when using AWS credentials)')
    parser.add_argument('--profile', help='AWS profile name')
    parser.add_argument('--region', help='AWS region', default="us-east-1")

    # Upload settings
    parser.add_argument('--part-size', type=int, default=100,
                       help='Size of each part for multipart upload in MB (default: 100)')
    parser.add_argument('--max-workers', type=int, default=10,
                       help='Maximum number of concurrent upload threads (default: 10)')

    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    # Validate arguments
    if not os.path.isfile(args.file_path):
        parser.error(f"File not found: {args.file_path}")

    if not args.url and not (args.bucket and args.key):
        parser.error("Either --url or both --bucket and --key must be provided")

    return args


def main():
    """Main function."""
    args = parse_arguments()

    # Set logging level based on verbose flag
    if args.verbose:
        logger.remove(0)  #  # Remove the first handler (stderr)
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="DEBUG"
        )
        logger.debug("Verbose logging enabled")

    # Create uploader with specified settings
    uploader = S3Uploader(part_size_mb=args.part_size, max_workers=args.max_workers)

    # Perform upload based on provided arguments
    success = False
    if args.url:
        logger.info(f"Uploading {args.file_path} using pre-signed URL")
        success = uploader.upload_with_presigned_url(args.file_path, args.url)
    else:
        logger.info(f"Uploading {args.file_path} to {args.bucket}/{args.key}")
        success = uploader.upload_with_aws_credentials(
            args.file_path,
            args.bucket,
            args.key,
            profile=args.profile,
            region=args.region
        )

    if success:
        logger.success("Upload completed successfully")
    else:
        logger.error("Upload failed")

    return 0 if success else 1


signal.signal(signal.SIGINT, handle_sigint)
if __name__ == "__main__":
    sys.exit(main())
