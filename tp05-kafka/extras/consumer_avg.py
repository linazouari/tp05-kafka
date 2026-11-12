
import json
import sys

from kafka import KafkaConsumer

TOPIC = "campus-sensors"
group = sys.argv[1] if len(sys.argv) > 1 else "tp05-avg-monitor"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=group,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

count, total = 0, 0.0
print(f"[{group}] listening on {TOPIC}")
try:
    for msg in consumer:
        e = msg.value
        count += 1
        total += e["temperature_c"]
        print(
            f'[{group}] p={msg.partition} off={msg.offset} {e["sensor_id"]} '
            f'T={e["temperature_c"]} | running avg = {total / count:.2f}°C ({count} events)'
        )
except KeyboardInterrupt:
    print("Stopped.")
finally:
    consumer.close()
