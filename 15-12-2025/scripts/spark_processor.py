from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, current_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType
import time
import sys

# --- Configuration --- #
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'

SOURCE_BUCKET = 'security-data-lake'
TARGET_PATH_CLOUD = f"s3a://{SOURCE_BUCKET}/processed/cloud_logs" # Cloud logs still go to MinIO
RAW_PATH_FIREWALL = f"s3a://{SOURCE_BUCKET}/raw_firewall_logs/*/*/*/*"
RAW_PATH_CLOUD = f"s3a://{SOURCE_BUCKET}/raw_cloud_logs/*/*/*/*"

# --- Spark Session Setup --- #
def get_spark_session():
    """Initializes and configures the SparkSession for MinIO (S3A)."""
    
    try:
        # Build Spark session with S3A configurations for MinIO
        spark = SparkSession.builder \
            .appName("SecurityLogProcessor") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        print(f"✓ SparkSession configured and connected to MinIO endpoint: {MINIO_ENDPOINT}")
        return spark
    except Exception as e:
        print(f"✗ Failed to create Spark session: {e}")
        return None

# --- Schema Definitions (Keep as is) --- #
def get_firewall_schema():
    """Define explicit schema for firewall logs."""
    return StructType([
        StructField("ts_unix", LongType(), True),
        StructField("src_ip", StringType(), True),
        StructField("dest_ip", StringType(), True),
        StructField("src_port", IntegerType(), True),
        StructField("dest_port", IntegerType(), True),
        StructField("protocol", StringType(), True),
        StructField("act", StringType(), True),
        StructField("bytes_sent", IntegerType(), True)
    ])

def get_cloud_schema():
    """Define explicit schema for cloud logs."""
    return StructType([
        StructField("event_time_iso", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("resource_name", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("success", BooleanType(), True),
        StructField("source_service", StringType(), True)
    ])

# --- Write to PostgreSQL Function --- #
def write_to_postgres(df, table_name):
    """Writes a DataFrame to PostgreSQL."""
    print(f"Writing {table_name} data to PostgreSQL...")

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    # Write the DataFrame
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite", # Use overwrite for a simple dashboard table
        properties=properties
    )
    print(f"✓ Successfully wrote {table_name} data to PostgreSQL table: {table_name}")
    
# --- Transformation Logic (Keep as is) --- #
def transform_firewall_logs(spark, raw_path):
    # ... (Keep the content of transform_firewall_logs as it was in your input) ...
    # This function should still return the transformed DataFrame (df_transformed)
    print("\n" + "="*60)
    print("Processing Firewall Logs")
    print("="*60)
    
    try:
        # Check if path exists
        try:
            spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).exists(
                spark._jvm.org.apache.hadoop.fs.Path(raw_path)
            )
        except:
            print(f"⚠ No data found at {raw_path}")
            return None
        
        # Read JSONL data (NOT multiLine - each line is a separate JSON object)
        df = spark.read \
            .schema(get_firewall_schema()) \
            .json(raw_path)
        
        record_count = df.count()
        
        if record_count == 0:
            print("⚠ No firewall records found to process")
            return None
            
        print(f"📊 Found {record_count} raw firewall records")
        
        # Transformation:
        df_transformed = df.select(
            # Convert milliseconds to timestamp (divide by 1000)
            from_unixtime(col("ts_unix") / 1000).cast("timestamp").alias("event_time"),
            
            # Anonymize last octet of source IP (e.g., 192.168.1.100 -> 192.168.1.XXX)
            regexp_replace(
                col("src_ip"), 
                r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.)\d{1,3}", 
                "$1XXX"
            ).alias("source_ip_anon"),
            
            col("dest_ip").alias("destination_ip"),
            col("src_port"),
            col("dest_port"),
            col("protocol"),
            col("act").alias("action"),
            col("bytes_sent"),
            current_timestamp().alias("processed_at")
        ).withColumn("log_type", lit("firewall"))
        
        # Show sample data
        print("\n📋 Sample transformed firewall data:")
        df_transformed.show(5, truncate=False)
        
        print(f"✓ Transformed {record_count} firewall records")
        return df_transformed
        
    except Exception as e:
        print(f"✗ Error processing firewall logs: {e}")
        import traceback
        traceback.print_exc()
        return None
# ... (Keep the content of transform_cloud_logs as it was in your input) ...
def transform_cloud_logs(spark, raw_path):
    """Reads raw cloud logs, transforms, and anonymizes sensitive PII."""
    print("\n" + "="*60)
    print("Processing Cloud Audit Logs")
    print("="*60)
    
    try:
        # Check if path exists
        try:
            spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).exists(
                spark._jvm.org.apache.hadoop.fs.Path(raw_path)
            )
        except:
            print(f"⚠ No data found at {raw_path}")
            return None
        
        # Read JSONL data
        df = spark.read \
            .schema(get_cloud_schema()) \
            .json(raw_path)
        
        record_count = df.count()
        
        if record_count == 0:
            print("⚠ No cloud audit records found to process")
            return None
            
        print(f"📊 Found {record_count} raw cloud audit records")
        
        # Transformation:
        df_transformed = df.select(
            col("event_time_iso").cast("timestamp").alias("event_time"),
            
            # PII Redaction: Replace user_id with REDACTED
            regexp_replace(col("user_id"), r".+", "REDACTED").alias("user_id_anon"),
            
            col("resource_name"),
            col("operation"),
            col("success"),
            col("source_service"),
            current_timestamp().alias("processed_at")
        ).withColumn("log_type", lit("cloud_audit"))
        
        # Show sample data
        print("\n📋 Sample transformed cloud data:")
        df_transformed.show(5, truncate=False)
        
        print(f"✓ Transformed {record_count} cloud audit records")
        return df_transformed
        
    except Exception as e:
        print(f"✗ Error processing cloud logs: {e}")
        import traceback
        traceback.print_exc()
        return None


# --- Main Logic --- #
def run_processor():
    print("\n" + "="*60)
    print("Security Log Processor - Starting")
    print("="*60)
    
    # Initialize Spark
    spark = get_spark_session()
    
    if not spark:
        print("✗ Spark Session failed to initialize. Exiting.")
        sys.exit(1)

    processed_any = False

    try:
        # Process Firewall Logs
        df_firewall = transform_firewall_logs(spark, RAW_PATH_FIREWALL)
        
        if df_firewall is not None and df_firewall.count() > 0:
            # --- CHANGE: WRITE TO POSTGRES FOR DASHBOARD ---
            write_to_postgres(df_firewall, "curated_firewall_logs") 
            # ------------------------------------------------
            processed_any = True
        else:
            print("⚠ Skipping firewall logs - no data to process")
        
        # Process Cloud Logs (Still writes to MinIO)
        df_cloud = transform_cloud_logs(spark, RAW_PATH_CLOUD)
        
        if df_cloud is not None and df_cloud.count() > 0:
            print(f"\n💾 Writing cloud logs to {TARGET_PATH_CLOUD}")
            
            # Write to MinIO as Parquet, partitioned by source service
            df_cloud.write \
                .mode("append") \
                .partitionBy("source_service") \
                .parquet(TARGET_PATH_CLOUD)
            
            final_count = df_cloud.count()
            print(f"✓ Successfully wrote {final_count} cloud audit records")
            processed_any = True
        else:
            print("⚠ Skipping cloud logs - no data to process")
        
        if not processed_any:
            print("\n⚠ No data was processed. Make sure the log generator and loader are running.")
            
    except Exception as e:
        print(f"\n✗ Fatal error during Spark processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()
        print("\n" + "="*60)
        print("Spark Session stopped")
        print("="*60)


if __name__ == "__main__":
    # Wait for loader to write initial data
    print("⏳ Waiting 15 seconds for data to be available...")
    time.sleep(15)
    
    run_processor()