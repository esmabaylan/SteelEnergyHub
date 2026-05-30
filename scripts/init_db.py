import psycopg2
import os
import time

# Docker ağında olduğumuz için localhost DEĞİL, container adını kullanıyoruz
DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "postgres",
    "password": "postgres"
}

# Docker içindeki dosya yolları
BASE_DIR = "/home/jovyan/work"

SQL_FILES = [
    f"{BASE_DIR}/database/init/00_enable_extractions.sql",
    f"{BASE_DIR}/database/init/01_create_schemas.sql",
    f"{BASE_DIR}/database/init/02_create_roles.sql",
    f"{BASE_DIR}/database/migrations/V001__initial_schema.sql",
    f"{BASE_DIR}/database/migrations/V002__anomaly_cost_tables.sql",
    f"{BASE_DIR}/database/migrations/V003__processed_readings.sql"
]

def initialize_database():
    print("==================================================")
    print("   TIMESCALEDB VERİTABANI KURULUMU BAŞLADI        ")
    print("==================================================")
    
    time.sleep(2)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        for sql_file in SQL_FILES:
            if os.path.exists(sql_file):
                print(f"[ŞEF] {sql_file.split('/')[-1]} çalıştırılıyor...")
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                    if sql_content.strip():
                        cur.execute(sql_content)
                print(f"[BAŞARILI] {sql_file.split('/')[-1]} tamamlandı.")
            else:
                print(f"[HATA] Dosya bulunamadı: {sql_file}")
                
        cur.close()
        conn.close()
        print("\n[TEBRİKLER] Bütün tablolar ve şemalar eksiksiz oluşturuldu!")
        
    except Exception as e:
        print(f"\n[KRİTİK HATA] Veritabanına bağlanırken hata oluştu:\n{e}")

if __name__ == "__main__":
    initialize_database()