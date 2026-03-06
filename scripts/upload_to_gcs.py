# import os

import argparse
import logging
from datetime import datetime, timezone

from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def upload_to_gcs(bucket_name: str, source_file_name: str, destination_blob_name: str) -> None:
    """
    Upload a local file to GCS.

    destination_blob_name should be an object path like:
      raw_data/car_sales_data_20260302T114655.csv
    (NOT a gs:// URI)
    """
    if destination_blob_name.startswith("gs://"):
        raise ValueError(
            "destination_blob_name must be an object path (e.g. raw_data/file.csv), not a gs:// URI."
        )

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    logger.info("Uploaded %s to gs://%s/%s", source_file_name, bucket_name, destination_blob_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Local File to GCS")
    parser.add_argument("--source_file", required=True, help="Path to the local source file.")
    parser.add_argument("--bucket_name", required=True, help="Name of the GCS bucket.")
    parser.add_argument(
        "--destination_blob_name",
        required=False,
        help="GCS object path (e.g. raw_data/car_sales_data_20260302.csv). "
             "If omitted, a UTC timestamp-based name is used.",
    )

    args = parser.parse_args()

    if args.destination_blob_name:
        destination = args.destination_blob_name
    else:
        # Fallback for manual runs (UTC is recommended for reproducibility)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        destination = f"raw_data/car_sales_data_{ts}.csv"

    try:
        upload_to_gcs(args.bucket_name, args.source_file, destination)
    except Exception:
        logger.exception("Failed uploading file to GCS")
        raise
