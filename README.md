# Fleet Telemetry Data Lake (AWS S3 + Spark + Athena)

## Project Goal

Build a lightweight **fleet telemetry data lake** that supports scalable analytics (fleet safety KPIs, driving behavior signals) using AWS Free Tier–friendly services.

This project simulates vehicle telemetry and demonstrates how raw telemetry can be ingested, processed, and queried using modern cloud data lake architecture.

---
## Project Highlights

- ✅ End-to-end AWS data lake pipeline for fleet telemetry  
- ✅ Partitioned S3 layout (`dt=...`) + Athena partition management  
- ✅ Spark ETL converting JSONL → Parquet  
- ✅ Fleet safety analytics (overspeed + harsh braking KPIs)  
---

## Why this project

Modern vehicles generate high-volume telemetry (speed, engine, GPS, events).  
This project demonstrates a practical **data lake pipeline** for storing raw telemetry cheaply (S3), converting it into **analytics-ready Parquet**, and enabling fast SQL analysis via **Athena**.

---
## Project Structure
```
fleet-telemetry-data-lake/
│
├── data/
│   ├── local_raw/
│   └── local_processed/
│
├── scripts/
│   └── generate_telemetry.py
│
├── spark_jobs/
│   ├── json_to_parquet_local.py
│   └── json_to_parquet_wsl_param.py
│
├── notebooks/
│
├── docs/
│
└── README.md
```
---
## Tech Stack

- AWS S3 (Data Lake Storage)
- Apache Spark / PySpark (ETL Processing)
- AWS Athena (SQL Analytics)
- Parquet (Columnar Storage)
- Python
- WSL2 (Development Environment)
---

## Architecture
```
Vehicle Telemetry Generator
        ↓
Bronze (RAW JSON)
S3 /bronze/telemetry/

        ↓ Spark ETL

Silver (Parquet optimized)
S3 /silver/telemetry/

        ↓

Athena SQL Analytics
Fleet Safety KPIs

```
---

```mermaid
flowchart TB
    A[Vehicle Telemetry Generator] --> B[S3 Data Lake RAW<br/>s3://bucket/raw/]

    B --> C[Spark ETL<br/>PySpark / AWS Glue]

    C --> D[S3 Data Lake PROCESSED<br/>Parquet format]

    D --> E[AWS Athena]

    E --> F[Fleet Safety Analytics<br/>Overspeed & Harsh Braking KPIs]
```

---

## Data Lake Layout (S3)

```
s3://fleet-datalake-pattarin/

raw/
 ├── dt=2026-03-01/
 ├── dt=2026-03-02/
 ├── dt=2026-03-03/
 └── dt=2026-03-04/

processed/
 └── telemetry/
      ├── dt=2026-03-01/
      ├── dt=2026-03-02/
      ├── dt=2026-03-03/
      └── dt=2026-03-04/
```
---

```mermaid
flowchart TB

subgraph Ingestion
A[Vehicle Telemetry Generator]
end

subgraph Data Lake
B[S3 RAW Zone<br/>JSON]
D[S3 PROCESSED Zone<br/>Parquet]
end

subgraph Processing
C[Spark ETL<br/>PySpark / AWS Glue]
end

subgraph Analytics
E[AWS Athena]
F[Fleet Safety KPIs]
end

A --> B
B --> C
C --> D
D --> E
E --> F
```
---
## Data Lake Zones

| Zone | Description |
|-----|-------------|
| Bronze | Raw JSON telemetry as ingested from vehicles |
| Silver | Cleaned, structured Parquet data optimized for analytics |

Bronze → Silver transformation is performed using PySpark.

---

## Telemetry Schema

### Core fields

| column | type |
|------|------|
vehicle_id | string
event_time | timestamp
speed_kmh | double
engine_rpm | int
gps_lat | double
gps_lon | double

### Safety signals

| column | description |
|------|------|
overspeed | 1 if speed > 130 km/h
harsh_brake | 1 if speed drop ≥ 20 km/h vs previous minute

---

## Reproduce the Pipeline

### 1 Generate Telemetry

```
python scripts/generate_telemetry.py --date 2026-03-04 --vehicles 5 --minutes 30
```

Output:

```
data/local_raw/dt=2026-03-04/telemetry.jsonl
```

---

### 2 Upload Raw Data to S3

```
aws s3 cp data/local_raw/dt=2026-03-04 \
s3://fleet-datalake-pattarin/raw/dt=2026-03-04/ \
--recursive
```

---

### 3 Convert JSONL → Parquet (Spark)

```
python spark_jobs/json_to_parquet_wsl_param.py --date 2026-03-04
```

Output:

```
data/local_processed/telemetry/dt=2026-03-04/
```

---

### 4 Upload Parquet to S3 Processed Layer

```
aws s3 cp data/local_processed/telemetry/dt=2026-03-04 \
s3://fleet-datalake-pattarin/processed/telemetry/dt=2026-03-04/ \
--recursive --exclude "*.crc"
```

---

### 5 Query Using Athena

Create table:

```
CREATE EXTERNAL TABLE fleet_lake.telemetry_parquet (
vehicle_id string,
event_time timestamp,
speed_kmh double,
engine_rpm int,
gps_lat double,
gps_lon double,
overspeed int,
harsh_brake int
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://fleet-datalake-pattarin/processed/telemetry/';
```

Register partitions:

```
MSCK REPAIR TABLE fleet_lake.telemetry_parquet;
```
![Athena Table Partitions](docs/images/AthenaQuery.png)
---

## Example Analytics Queries

### Fleet Safety Analytics :Overspeed leaderboard
Overspeed events are calculated using the `overspeed` flag generated during ETL.

```
SELECT
dt,
vehicle_id,
SUM(overspeed) AS overspeed_events,
ROUND(100.0 * SUM(overspeed) / COUNT(*), 2) AS overspeed_pct,
MAX(speed_kmh) AS max_speed_kmh
FROM fleet_lake.telemetry_parquet
WHERE dt = '2026-03-04'
GROUP BY dt, vehicle_id
ORDER BY overspeed_events DESC;
```
![Athena Overspeed Query](docs/images/OverspeedLeaderboard.png)

The query identifies vehicles with the highest overspeed event rate.
---

### Driving Behavior Detection: Harsh braking events
Harsh braking events are detected when vehicle speed drops by ≥ 20 km/h between consecutive telemetry points.

```
SELECT
dt,
vehicle_id,
SUM(harsh_brake) AS harsh_brake_events
FROM fleet_lake.telemetry_parquet
GROUP BY dt, vehicle_id
ORDER BY dt, harsh_brake_events DESC;
```
![Athena Overspeed Query](docs/images/vehicleid_overspeed.png)
---

## Current Dataset

Partitions:

```
dt=2026-03-01
dt=2026-03-02
dt=2026-03-03
dt=2026-03-04
```

Each partition contains:

```
5 vehicles
30 minutes telemetry
150 rows per day
```

---

## Cost Considerations

This project runs within AWS Free Tier when used carefully.

S3 storage: small synthetic dataset  
Athena: pay-per-query (~$5 per TB scanned)  
Parquet format reduces scan cost significantly.

---

## Future Improvements

Possible extensions:

• Run ETL using AWS Glue instead of local Spark  
• Add streaming ingestion using Kafka or Kinesis  
• Add fleet anomaly detection  
• Build dashboards using Amazon QuickSight

---

## Key Takeaways

- Designed a **partitioned data lake** on S3 (RAW JSONL → PROCESSED Parquet) using `dt=YYYY-MM-DD` partitions.
- Built a repeatable ETL workflow with **Spark (PySpark)** to transform and optimize telemetry for analytics.
- Implemented **fleet safety signals** during ETL:
  - `overspeed` (speed > 130 km/h)
  - `harsh_brake` (speed drop ≥ 20 km/h between consecutive points)
- Queried processed telemetry using **Athena SQL** to produce fleet KPIs:
  - overspeed leaderboard
  - harsh braking counts per vehicle/day
- Used Parquet to reduce query cost and improve performance compared to scanning raw JSON.
---
## Skills Demonstrated

- AWS: S3, Athena (and Glue-ready architecture)
- Data Lake architecture (RAW → PROCESSED)
- Partitioning strategy for incremental data ingestion
- Spark ETL with PySpark
- Parquet columnar storage optimization
- SQL analytics for fleet telemetry KPIs
---

## Next Improvements (Roadmap)

- Make ETL fully AWS-native with **AWS Glue** (Spark on AWS) and job scheduling.
- Add schema evolution and data quality checks (missing GPS, invalid speed, timestamp gaps).
- Add more telemetry fields (battery/temperature, idle time, trip distance) and anomaly detection queries.
- Create a simple dashboard (QuickSight / Streamlit) for fleet safety monitoring.
---

## Author

**Pattarin Thunyapar**  
M.Sc. Data Analytics (Berlin) • Data Engineering / Analytics • AWS Data Lakes  
- Interests: Fleet telemetry, IoT data platforms, Spark ETL, cloud analytics, data quality & partitioning  
- Location: Berlin, Germany (open to EU roles / remote)
