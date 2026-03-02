import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD, e.g. 2026-03-02")
    args = p.parse_args()
    dt = args.date

    input_path = f"data/local_raw/dt={dt}/telemetry.jsonl"
    output_path = f"data/local_processed/telemetry/dt={dt}"

    Path("data/local_processed/telemetry").mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("FleetTelemetry_WSL_JSON_to_Parquet").getOrCreate()

    df = spark.read.json(input_path)
    df = df.withColumn("event_time", to_timestamp("event_time"))

    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Wrote Parquet for dt={dt} to: {output_path}")
    print("Rows:", df.count())
    spark.stop()

if __name__ == "__main__":
    main()
