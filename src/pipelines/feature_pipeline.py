import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}

NIGHT_TARIFF = 1.80
DAY_TARIFF   = 3.20
INTERVAL_SEC = 20  # Her 20 saniyede bir kontrol et


def get_unprocessed(cur, last_processed_time):
    """energy_readings'den henüz işlenmemiş kayıtları çek."""
    cur.execute("""
        SELECT e.*
        FROM main_data.energy_readings e
        LEFT JOIN main_data.processed_readings p ON e.time = p.time
        WHERE p.time IS NULL
          AND e.time > %s
        ORDER BY e.time
        LIMIT 500
    """, (last_processed_time,))
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)


def extract_features(df):
    """Özellik çıkarımı yap."""
    if df.empty:
        return df

    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time').reset_index(drop=True)

    # Tarih/saat özellikleri
    df['hour']       = df['time'].dt.hour
    df['day']        = df['time'].dt.day
    df['month']      = df['time'].dt.month
    df['quarter']    = df['time'].dt.quarter
    df['is_weekend'] = df['time'].dt.weekday >= 5

    df['shift'] = df['hour'].apply(lambda h:
        'night_shift'   if h < 8   else
        'day_shift'     if h < 16  else
        'evening_shift'
    )

    df['tariff_period'] = df['hour'].apply(lambda h:
        'night' if (h >= 22 or h < 6) else 'day'
    )

    df['day_period'] = df['hour'].apply(lambda h:
        'midnight'  if h < 6   else
        'morning'   if h < 12  else
        'afternoon' if h < 18  else
        'evening'
    )

    # Enerji özellikleri
    df['usage_ma_1h']      = df['usage_kwh'].rolling(window=4).mean()
    df['usage_ma_4h']      = df['usage_kwh'].rolling(window=16).mean()
    df['usage_diff']       = df['usage_kwh'].diff()
    df['usage_pct_change'] = df['usage_kwh'].pct_change() * 100

    # Güç faktörü özellikleri
    df['pf_efficiency']          = (df['lagging_pf'] + df['leading_pf']) / 2
    df['reactive_power_balance'] = df['lagging_reactive_power'] - df['leading_reactive_power']
    df['low_pf_flag']            = df['lagging_pf'] < 80

    # Anomali özellikleri
    # Anomali özellikleri - sabit tarihsel istatistikler kullan
    USAGE_MEAN = 27.39
    USAGE_STD = 33.44

    df['z_score'] = (df['usage_kwh'] - USAGE_MEAN) / USAGE_STD

    # Spike (yüksek) VEYA Drop (çok düşük) anomali
    df['z_anomaly'] = (df['z_score'] > 3) | (df['usage_kwh'] < 2.0)

    #mean = df['usage_kwh'].mean()
    #std  = df['usage_kwh'].std() if df['usage_kwh'].std() > 0 else 1
    #df['z_score']  = (df['usage_kwh'] - mean) / std
    #df['z_anomaly'] = df['z_score'].abs() > 3

    # Maliyet özellikleri
    df['cost_tl'] = df['tariff_period'].apply(
        lambda x: NIGHT_TARIFF if x == 'night' else DAY_TARIFF
    ) * df['usage_kwh']
    df['cost_saving'] = df['cost_tl'] - (df['usage_kwh'] * NIGHT_TARIFF)

    return df.dropna(subset=['usage_ma_1h'])


def insert_processed(cur, df):
    """İşlenmiş veriyi processed_readings tablosuna yaz."""
    cols = [
        'time', 'usage_kwh', 'lagging_reactive_power', 'leading_reactive_power',
        'co2_tco2', 'lagging_pf', 'leading_pf', 'nsm', 'week_status',
        'day_of_week', 'load_type', 'hour', 'day', 'month', 'quarter',
        'is_weekend', 'shift', 'tariff_period', 'day_period',
        'usage_ma_1h', 'usage_ma_4h', 'usage_diff', 'usage_pct_change',
        'pf_efficiency', 'reactive_power_balance', 'low_pf_flag',
        'z_score', 'z_anomaly', 'cost_tl', 'cost_saving'
    ]

    df_insert = df[cols].copy()
    rows = [tuple(row) for row in df_insert.itertuples(index=False)]
    if not rows:
        return 0

    placeholders = ','.join(['%s'] * len(cols))
    execute_batch(cur, f"""
        INSERT INTO main_data.processed_readings ({','.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """, rows, page_size=500)

    return len(rows)


def run():
    print("=" * 50)
    print("Steel Energy Hub — Feature Pipeline")
    print(f"Interval: {INTERVAL_SEC}s")
    print("=" * 50)
    print("Durdurmak için Ctrl+C\n")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    # Son işlenmiş zamanı bul
    cur.execute("SELECT MAX(time) FROM main_data.processed_readings;")
    result = cur.fetchone()[0]
    last_time = result if result else datetime(2000, 1, 1)
    print(f"Son işlenmiş zaman: {last_time}\n")

    total_processed = 0

    try:
        while True:
            df = get_unprocessed(cur, last_time)

            if df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Yeni kayıt yok, bekleniyor...")
            else:
                df = extract_features(df)
                count = insert_processed(cur, df)
                total_processed += count

                if not df.empty:
                    last_time = df['time'].max()

                anomali = df['z_anomaly'].sum() if 'z_anomaly' in df.columns else 0
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"{count} kayıt işlendi | "
                      f"{anomali} anomali | "
                      f"Toplam: {total_processed}")

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\nDurduruldu. Toplam işlenen: {total_processed} kayıt")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()