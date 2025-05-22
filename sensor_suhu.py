from kafka import KafkaProducer
import json
import time
import random

# Konfigurasi Kafka
bootstrap_servers = 'localhost:9092'
topic_name = 'sensor-suhu-gudang'

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_ids = ["G1", "G2", "G3"]

print(f"Mengirim data suhu ke topik: {topic_name}...")

try:
    while True:
        for gudang_id in gudang_ids:
            suhu = random.randint(70, 90)  # Suhu antara 70 dan 90 C

            data = {
                "gudang_id": gudang_id,
                "suhu": suhu,
                "timestamp": time.time()  # Timestamp event
            }

            producer.send(topic_name, value=data)
            print(f"Terkirim (Suhu): {data}")

        time.sleep(1)  # Kirim batch setiap 1 detik

except KeyboardInterrupt:
    print("Producer suhu dihentikan.")
finally:
    producer.flush()
    producer.close()
