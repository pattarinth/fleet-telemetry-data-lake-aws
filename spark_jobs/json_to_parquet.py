from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("FleetTelemetryETL") \
    .getOrCreate()

# Read raw JSON from S3
raw_path = "s3://fleet-datalake-pattarin-4200-eu-central-1/raw/"

df = spark.read.json(raw_path)

# Convert event_time to timestamp
from pyspark.sql.functions import to_timestamp

df = df.withColumn(
    "event_time",
    to_timestamp("event_time")
)

# Write to Parquet (processed layer)
output_path = "s3://fleet-datalake-pattarin-4200-eu-central-1/processed/telemetry/"

df.write \
  .mode("overwrite") \
  .parquet(output_path)

print("Parquet files written to:", output_path)

spark.stop()