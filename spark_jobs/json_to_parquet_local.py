from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Local paths (Windows-friendly)
INPUT_PATH = r"data/local_raw/dt=2026-03-01/telemetry.jsonl"
OUTPUT_PATH = r"data/local_processed/telemetry/dt=2026-03-01"

spark = (
    SparkSession.builder
    .appName("FleetTelemetry_Local_JSON_to_Parquet")
    .getOrCreate()
)

# Read newline-delimited JSON
df = spark.read.json(INPUT_PATH)

# Parse event_time (string -> timestamp)
df = df.withColumn("event_time", to_timestamp("event_time"))

# Write Parquet locally
(
    df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("✅ Wrote local Parquet to:", OUTPUT_PATH)
print("Rows:", df.count())
print("Schema:")
df.printSchema()

spark.stop()