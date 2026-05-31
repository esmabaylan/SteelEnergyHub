import random
import time
from datetime import datetime
import psycopg2

# Gerçek veriden çıkarılan istatistikler
STATS = {
    "usage_kwh":              {"mean": 27.39, "std": 33.44, "min": 0, "max": 157.18},
    "lagging_reactive_power": {"mean": 13.04, "std": 16.31, "min": 0, "max": 80.0},
    "leading_reactive_power": {"mean": 0.5,   "std": 2.0,   "min": 0, "max": 20.0},
    "co2_tco2":               {"mean": 0.012, "std": 0.016, "min": 0, "max": 0.08},
    "lagging_pf":             {"mean": 80.58, "std": 18.92, "min": 0, "max": 100.0},
    "leading_pf":             {"mean": 95.0,  "std": 10.0,  "min": 0, "max": 100.0},
}

DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}

INTERVAL_SEC = 5 #15
ANOMALY_RATE = 0.006

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
LOAD_TYPES = ["Light_Load", "Medium_Load", "Maximum_Load"]
LOAD_WEIGHTS = [0.52, 0.28, 0.20]


def clamp(value, min_val, max_val):
    return max(min_val, min(max_val, value))


def generate_reading():
    now = datetime.now()
    hour = now.hour
    day_of_week = DAYS[now.weekday()]
    week_status = "Weekend" if now.weekday() >= 5 else "Weekday"
    nsm = hour * 3600 + now.minute * 60 + now.second
    is_anomaly = random.random() < ANOMALY_RATE

    if is_anomaly:
        anomaly_type = random.choice(["spike", "drop"])
        if anomaly_type == "spike":
            usage_kwh = random.uniform(
                STATS["usage_kwh"]["mean"] + 3 * STATS["usage_kwh"]["std"],
                STATS["usage_kwh"]["max"]
            )
        else:
            usage_kwh = random.uniform(0, 2.0)
        load_type = random.choices(LOAD_TYPES, weights=LOAD_WEIGHTS)[0]
    else:
        if 9 <= hour <= 16:
            usage_mean = STATS["usage_kwh"]["mean"] * 2.1
            load_type = random.choices(LOAD_TYPES, weights=[0.20, 0.35, 0.45])[0]
        elif 7 <= hour <= 9 or 16 <= hour <= 20:
            usage_mean = STATS["usage_kwh"]["mean"] * 1.4
            load_type = random.choices(LOAD_TYPES, weights=LOAD_WEIGHTS)[0]
        else:
            usage_mean = STATS["usage_kwh"]["mean"] * 0.3
            load_type = random.choices(LOAD_TYPES, weights=[0.80, 0.15, 0.05])[0]

        usage_kwh = clamp(
            random.gauss(usage_mean, STATS["usage_kwh"]["std"] * 0.4),
            STATS["usage_kwh"]["min"],
            STATS["usage_kwh"]["max"]
        )

    return {
        "time":                   now,
        "usage_kwh":              round(usage_kwh, 2),
        "lagging_reactive_power": round(clamp(random.gauss(
            STATS["lagging_reactive_power"]["mean"],
            STATS["lagging_reactive_power"]["std"]), 0, 80), 2),
        "leading_reactive_power": round(clamp(random.gauss(
            STATS["leading_reactive_power"]["mean"],
            STATS["leading_reactive_power"]["std"]), 0, 20), 2),
        "co2_tco2":               round(clamp(random.gauss(
            STATS["co2_tco2"]["mean"],
            STATS["co2_tco2"]["std"]), 0, 0.08), 4),
        "lagging_pf":             round(clamp(random.gauss(
            STATS["lagging_pf"]["mean"],
            STATS["lagging_pf"]["std"]), 0, 100), 2),
        "leading_pf":             round(clamp(random.gauss(
            STATS["leading_pf"]["mean"],
            STATS["leading_pf"]["std"]), 0, 100), 2),
        "nsm":                    nsm,
        "week_status":            week_status,
        "day_of_week":            day_of_week,
        "load_type":              load_type,
        "is_anomaly":             is_anomaly
    }


def insert_reading(cur, r):
    cur.execute("""
        INSERT INTO main_data.energy_readings
        (time, usage_kwh, lagging_reactive_power, leading_reactive_power,
         co2_tco2, lagging_pf, leading_pf, nsm, week_status, day_of_week, load_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (
        r["time"], r["usage_kwh"], r["lagging_reactive_power"],
        r["leading_reactive_power"], r["co2_tco2"], r["lagging_pf"],
        r["leading_pf"], r["nsm"], r["week_status"],
        r["day_of_week"], r["load_type"]
    ))


def run():
    print("=" * 50)
    print("Steel Energy Hub — Dummy Data Generator")
    print(f"Interval: {INTERVAL_SEC}s | Anomaly Rate: {ANOMALY_RATE*100:.2f}%")
    print("=" * 50)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    generated = 0

    try:
        while True:
            r = generate_reading()
            insert_reading(cur, r)
            generated += 1

            status = "ANOMALY" if r["is_anomaly"] else "Normal"
            print(f"[{r['time'].strftime('%H:%M:%S')}] {status} | "
                  f"usage_kwh: {r['usage_kwh']} | "
                  f"load: {r['load_type']}")

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\nStopped. Total generated: {generated} records")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()