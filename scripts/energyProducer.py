import psycopg2
import json
import time
from datetime import datetime
from confluent_kafka import Producer

DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}

KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:9092"
}

TOPIC = "energy_topic"
BATCH_SIZE = 100   # Her seferinde kaç satır çekilsin
SLEEP_SEC = 0.0    # Mesajlar arası bekleme (saniye)


def delivery_report(err, msg):
    if err:
        print(f"ERROR:Don't send to message  {err}")
    else:
        pass  


def serialize(row):
    return json.dumps({
        "time":                   row[0].isoformat() if row[0] else None,
        "usage_kwh":              row[1],
        "lagging_reactive_power": row[2],
        "leading_reactive_power": row[3],
        "co2_tco2":               row[4],
        "lagging_pf":             row[5],
        "leading_pf":             row[6],
        "nsm":                    row[7],
        "week_status":            row[8],
        "day_of_week":            row[9],
        "load_type":              row[10],
    }, ensure_ascii=False)


def run():
    print("=" * 50)
    print("Steel Energy Hub — Kafka Producer")
    print("=" * 50)

    # Veritabanı bağlantısı
    print("\n[1] Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("    Connection established.")

    # Toplam satır sayısı
    cur.execute("SELECT COUNT(*) FROM main_data.energy_readings;")
    total = cur.fetchone()[0]
    print(f"    Total Lines: {total}")

    # Kafka producer
    print("\n[2] Initializing Kafka producer...")
    producer = Producer(KAFKA_CONFIG)
    print(f"    Topic: {TOPIC}")

    # Veriyi sırayla çek ve Kafka'ya gönder
    print(f"\n[3] Starting data transmission ({SLEEP_SEC}s intervals)...")
    print("    Durdurmak için Ctrl+C\n")

    cur.execute("""
        SELECT time, usage_kwh, lagging_reactive_power, leading_reactive_power,
               co2_tco2, lagging_pf, leading_pf, nsm, week_status, day_of_week, load_type
        FROM main_data.energy_readings
        ORDER BY time
    """)

    sent = 0
    start_time = time.time()

    try:
        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                message = serialize(row)
                producer.produce(
                    topic=TOPIC,
                    key=str(row[0]),   # timestamp key olarak
                    value=message,
                    callback=delivery_report
                )
                sent += 1

                # Her 500 mesajda bir özet yaz
                if sent % 500 == 0:
                    elapsed = time.time() - start_time
                    print(f"  Sent: {sent}/{total} | "
                          f"Time: {elapsed:.1f}s | "
                          f"Speed: {sent/elapsed:.0f} msg/s | "
                          f"Last: {row[0]}")

                producer.poll(0)
                time.sleep(SLEEP_SEC)

            producer.flush()

    except KeyboardInterrupt:
        print(f"\n  User stopped. Sent: {sent}")

    finally:
        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"Completed!")
        print(f"  Total sent        : {sent} messages")
        print(f"  Total time        : {elapsed:.1f} seconds")
        print(f"  Average speed     : {sent/elapsed:.1f} msg/s")
        cur.close()
        conn.close()
        producer.flush()


if __name__ == "__main__":
    run()