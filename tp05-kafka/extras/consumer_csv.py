 """Consumer that filters one building (optional) and saves events to CSV.
Usage: python extras/consumer_csv.py [building]
"""
import csv
import json
import sys

from kafka import KafkaConsumer

TOPIC = "campus-sensors"
building = sys.argv[1] if len(sys.argv) > 1 else None
FIELDS = ["event_id", "sensor_id", "temperature_c", "humidity_pct", "timestamp"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-csv-writer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

with open("events.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()
    print("Writing to events.csv (Ctrl+C to stop)")
    try:
        for msg in consumer:
            e = msg.value
            if building and e.get("campus_building") != building:
                continue
            writer.writerow(e)
            f.flush()
            print("Saved:", e["event_id"])
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        consumer.close()
