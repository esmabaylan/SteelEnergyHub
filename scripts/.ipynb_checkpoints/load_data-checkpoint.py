import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import sys

# --- Bağlantı ayarları ---
DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}

CSV_PATH = "data\\raw\\Steel_industry_data.csv"

def load_csv():
    print("CSV okunuyor...")
    df = pd.read_csv(CSV_PATH)
    print(f"  {len(df)} satır, {len(df.columns)} kolon okundu.")
    print(f"  Kolonlar: {list(df.columns)}")
    return df

def clean_data(df):
    print("Veri temizleniyor...")

    df.columns = [c.strip().lower()
                   .replace(" ", "_")
                   .replace(".", "_")
                   .replace("(", "")
                   .replace(")", "") for c in df.columns]

    print(f"  Temizlenmiş kolonlar: {list(df.columns)}")

    date_col = "date"
    try:
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)
    except Exception:
        df[date_col] = pd.to_datetime(df[date_col], infer_datetime_format=True)

    print(f"  Tarih aralığı: {df[date_col].min()} → {df[date_col].max()}")


    null_counts = df.isnull().sum()
    if null_counts.any():
        print(f"  Null değerler:\n{null_counts[null_counts > 0]}")
    else:
        print("  Null değer yok.")

    return df

def insert_data(df):
    print("Veritabanına bağlanılıyor...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    col_map = {
        "date":                         "time",
        "usage_kwh":                    "usage_kwh",
        "lagging_current_reactive_power_kvarh": "lagging_reactive_power",
        "leading_current_reactive_power_kvarh": "leading_reactive_power",
        "co2tco2":                      "co2_tco2",
        "lagging_current_power_factor": "lagging_pf",
        "leading_current_power_factor": "leading_pf",
        "nsm":                          "nsm",
        "weekstatus":                   "week_status",
        "day_of_week":                  "day_of_week",
        "load_type":                    "load_type",
    }


    available = {k: v for k, v in col_map.items() if k in df.columns}
    missing = [k for k in col_map if k not in df.columns]
    if missing:
        print(f"  Uyarı — CSV'de eksik kolonlar: {missing}")

    df_insert = df[list(available.keys())].rename(columns=available)

    db_cols = list(df_insert.columns)
    placeholders = ", ".join(["%s"] * len(db_cols))
    col_names = ", ".join(db_cols)

    insert_sql = f"""
        INSERT INTO main_data.energy_readings ({col_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """

    rows = [tuple(row) for row in df_insert.itertuples(index=False)]

    print(f"  {len(rows)} rows loaded (batch=1000)...")
    execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()

    cur.close()
    conn.close()
    print("  Completion.")

def verify():
    print("verifying...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM main_data.energy_readings;")
    count = cur.fetchone()[0]
    print(f"  Total rows in table: {count}")

    cur.execute("""
        SELECT MIN(time), MAX(time)
        FROM main_data.energy_readings;
    """)
    min_t, max_t = cur.fetchone()
    print(f"  Date range: {min_t} → {max_t}")

    cur.execute("""
        SELECT load_type, COUNT(*)
        FROM main_data.energy_readings
        GROUP BY load_type
        ORDER BY COUNT(*) DESC;
    """)
    print("  Load Type :")
    for row in cur.fetchall():
        print(f"    {row[0]}: {row[1]} rows")

    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        df = load_csv()
        df = clean_data(df)
        insert_data(df)
        verify()
        print("\Completion!")
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)