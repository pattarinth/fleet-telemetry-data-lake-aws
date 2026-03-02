from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pathlib import Path

INPUT_PATH = "data/local_raw/dt=2026-03-01/telemetry.jsonl"
OUTPUT_PATH = "data/local_processed/telemetry/dt=2026-03-01"

# Ensure output base exists
Path("data/local_processed/telemetry").mkdir(parents=True, exist_ok=True)

spark = SparkSession.builder.appName("FleetTelemetry_WSL_JSON_to_Parquet").getOrCreate()

df = spark.read.json(INPUT_PATH)
df = df.withColumn("event_time", to_timestamp("event_time"))

df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("✅ Wrote Parquet to:", OUTPUT_PATH)
print("Rows:", df.count())
df.printSchema()

spark.stop()
