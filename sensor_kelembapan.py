from kafka import KafkaProducer
import json
import time
import random

# Konfigurasi Kafka
bootstrap_servers = 'localhost:9092'
topic_name = 'sensor-kelembaban-gudang'

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_ids = ["G1", "G2", "G3"]

print(f"Mengirim data kelembaban ke topik: {topic_name}...")

try:
    while True:
        for gudang_id in gudang_ids:
            kelembaban = random.randint(60, 85)  # Kelembaban antara 60% dan 85%

            data = {
                "gudang_id": gudang_id,
                "kelembaban": kelembaban,
                "timestamp": time.time()  # Timestamp event
            }

            producer.send(topic_name, value=data)
            print(f"Terkirim (Kelembaban): {data}")

        time.sleep(1)  # Kirim batch setiap 1 detik

except KeyboardInterrupt:
    print("Producer kelembaban dihentikan.")
finally:
    producer.flush()
    producer.close()
