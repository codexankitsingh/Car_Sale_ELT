import os
from google.cloud import storage
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        # The storage client automatically looks for the GOOGLE_APPLICATION_CREDENTIALS
        # environment variable.
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        logger.info(
            f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}."
        )
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Local File to GCS")
    parser.add_argument("--source_file", required=True, help="Path to the local source file.")
    parser.add_argument("--bucket_name", required=True, help="Name of the GCS bucket.")
    args = parser.parse_args()

    Now = datetime.now()
    year = Now.strftime('%Y')
    month = Now.strftime('%m')
    day = Now.strftime('%d')
    timestamp = Now.strftime('%Y%m%d_%H%M%S')
    
    destination = f"raw_data/{year}/{month}/{day}/car_sales_data_{timestamp}.csv"
    upload_to_gcs(args.bucket_name, args.source_file, destination)
