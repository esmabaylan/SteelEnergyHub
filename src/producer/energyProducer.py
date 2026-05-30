import psycopg2
import json
import time
from datetime import datetime
from confluent_kafka import Producer
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}

KAFKA_CONFIG = {"bootstrap.servers": "kafka:9092"}
TOPIC = "energy_raw"
BATCH_SIZE = 100
SLEEP_SEC = 0.5


def delivery_report(err, msg):
    if err:
        print(f"  [HATA] {err}")


def serialize(row, cols):
    data = dict(zip(cols, row))
    for k, v in data.items():
        if hasattr(v, 'isoformat'):
            data[k] = v.isoformat()
        elif hasattr(v, 'item'):
            data[k] = v.item()
    return json.dumps(data, ensure_ascii=False, default=str)
def run():
    print("=" * 50)
    print("Steel Energy Hub — Kafka Producer")
    print("Kaynak: main_data.processed_readings")
    print("=" * 50)

    conn = psycopg2.connect(**DB_CONFIG)
    producer = Producer(KAFKA_CONFIG)
    sent = 0

    # Son gönderilen timestamp'i takip et
    #last_sent_time = datetime(2018, 1, 1)
    last_sent_time = datetime.now() - timedelta(hours=1)

    try:
        while True:
            cur = conn.cursor()
            cur.execute("""
                SELECT *
                FROM main_data.processed_readings
                WHERE time > %s
                ORDER BY time
                LIMIT 100
            """, (last_sent_time,))

            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            cur.close()

            if rows:
                for row in rows:
                    message = serialize(row, cols)
                    producer.produce(
                        topic=TOPIC,
                        key=str(row[0]),
                        value=message,
                        callback=delivery_report
                    )
                    sent += 1
                    producer.poll(0)

                last_sent_time = rows[-1][0]
                producer.flush()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"{len(rows)} mesaj gönderildi | "
                      f"Toplam: {sent} | "
                      f"Son: {last_sent_time}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Yeni kayıt yok, bekleniyor...")

            time.sleep(15)  # 15 saniyede bir kontrol et

    except KeyboardInterrupt:
        print(f"\nDurduruldu. Toplam gönderilen: {sent}")
    finally:
        conn.close()
        producer.flush()

if __name__ == "__main__":
    run()