import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, current_timestamp, date_format, to_date, trunc, substring, to_timestamp
from pyspark.sql.types import StringType

def main(gcs_input_path, gcs_output_path):
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CarSalesDataTransformation") \
        .getOrCreate()
        
    print(f"Reading raw data from: {gcs_input_path}")
    
    # 1. READ RAW DATA FROM GCS
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(gcs_input_path)
    print(f"Initial Row Count: {df_raw.count()}")

    # 2. DATA TRANSFORMATIONS
    # A. Clean Data: Replace nulls/blanks with "NA" in string columns
    clean_df = df_raw
    for curr_col in clean_df.columns:
        clean_df = clean_df.withColumn(
            curr_col, 
            when(col(curr_col).isNull() | (col(curr_col) == "") | (col(curr_col) == " "), "NA")
            .otherwise(col(curr_col).cast(StringType()))
        )

    # B & C. Parse Date and Time, Create Sales YearMonth (DATE type for BQ)
    # saledate format example: 'Tue Dec 16 2014 12:30:00 GMT-0800 (PST)'
    
    # Extract the first 24 chars only if it's not "NA", else set NULL
    clean_dt_str = when(col("saledate") != "NA", substring(col("saledate"), 1, 24)).otherwise(None)
    
    # Parse the timestamp (will silently evaluate to NULL if clean_dt_str is NULL)
    parsed_timestamp = to_timestamp(clean_dt_str, "EEE MMM dd yyyy HH:mm:ss")

    transformed_df = clean_df \
        .withColumn("sale_date", to_date(parsed_timestamp)) \
        .withColumn("sale_time", date_format(parsed_timestamp, "HH:mm:ss")) \
        .withColumn("sales_yearmonth", trunc(to_date(parsed_timestamp), "month")) \
        .drop("saledate")

    # D. Add ETL Timestamp
    final_df = transformed_df.withColumn("etl_timestamp", current_timestamp())

    print("Transformation Complete. Sample Data:")
    final_df.show(5, truncate=False)

    # 3. WRITE TRANSFORMED DATA TO GCS (PROCESSED FOLDER)
    print(f"Writing processed data to: {gcs_output_path}")
    final_df.write \
        .mode("overwrite") \
        .parquet(gcs_output_path)
        
    print("Write successful!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit dataproc_transformation.py <gcs_input_path> <gcs_output_path>")
        sys.exit(1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
