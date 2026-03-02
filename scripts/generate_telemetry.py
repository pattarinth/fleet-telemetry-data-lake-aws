import json
import random
from datetime import datetime, timedelta
from pathlib import Path
import argparse


def generate_vehicle_data(vehicle_id, start_time, minutes):
    data = []

    lat = 52.52
    lon = 13.405
    prev_speed = None

    for i in range(minutes):
        timestamp = start_time + timedelta(minutes=i)

        speed = max(0, (random.gauss(145, 15) if random.random() < 0.30 else random.gauss(60, 20)))
        rpm = int(speed * 40 + random.randint(-200, 200))

        lat += random.uniform(-0.0005, 0.0005)
        lon += random.uniform(-0.0005, 0.0005)

        overspeed = 1 if speed > 130 else 0
        harsh_brake = 1 if (prev_speed is not None and (prev_speed - speed) >= 20) else 0
        prev_speed = speed

        record = {
            "vehicle_id": vehicle_id,
            "event_time": timestamp.isoformat(),
            "speed_kmh": round(speed, 2),
            "engine_rpm": rpm,
            "gps_lat": round(lat, 6),
            "gps_lon": round(lon, 6),
            "overspeed": overspeed,
            "harsh_brake": harsh_brake,
        }

        data.append(record)

    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--vehicles", type=int, default=5)
    parser.add_argument("--minutes", type=int, default=30)
    args = parser.parse_args()

    output_dir = Path(f"data/local_raw/dt={args.date}")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / "telemetry.jsonl"

    start_time = datetime.strptime(args.date + " 08:00", "%Y-%m-%d %H:%M")

    with open(file_path, "w", encoding="utf-8") as f:
        for v in range(1, args.vehicles + 1):
            vehicle_id = f"DE-FLEET-{v:03d}"
            records = generate_vehicle_data(vehicle_id, start_time, args.minutes)
            for r in records:
                f.write(json.dumps(r) + "\n")

    print(f"✅ Telemetry written to {file_path}")


if __name__ == "__main__":
    main()
