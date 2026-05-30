import pytest
import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}


@pytest.fixture
def conn():
    connection = psycopg2.connect(**DB_CONFIG)
    yield connection
    connection.close()


def test_anomalies_being_written(conn):
    """Anomaliler DB'ye yazılıyor mu?"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM main_data.anomalies;")
    count = cur.fetchone()[0]
    assert count > 0, "Anomali tablosu boş — Spark streaming çalışıyor mu?"
    print(f"  Toplam anomali: {count}")
    cur.close()


def test_cost_analysis_being_written(conn):
    """Maliyet analizi DB'ye yazılıyor mu?"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM main_data.cost_analysis;")
    count = cur.fetchone()[0]
    assert count > 0, "cost_analysis tablosu boş — Spark streaming çalışıyor mu?"
    print(f"  Toplam maliyet kaydı: {count}")
    cur.close()


def test_cost_tariff_types(conn):
    """Maliyet tablosunda gece/gündüz tarifeler var mı?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT tariff_type FROM main_data.cost_analysis;
    """)
    tariff_types = [row[0] for row in cur.fetchall()]
    assert "night" in tariff_types, "Gece tarifesi kaydı yok!"
    assert "day" in tariff_types, "Gündüz tarifesi kaydı yok!"
    cur.close()


def test_anomaly_z_score(conn):
    """Anomalilerin z_score değeri gerçekten > 3 mü?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM main_data.anomalies
        WHERE ABS(z_score) <= 3;
    """)
    invalid = cur.fetchone()[0]
    assert invalid == 0, f"{invalid} adet z_score <= 3 olan anomali var!"
    cur.close()


def test_pipeline_lag(conn):
    """Feature pipeline geride mi? processed_readings güncel mi?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(time) FROM main_data.processed_readings;
    """)
    last_processed = cur.fetchone()[0]
    cur.execute("""
        SELECT MAX(time) FROM main_data.energy_readings;
    """)
    last_raw = cur.fetchone()[0]

    if last_processed and last_raw:
        lag = last_raw - last_processed
        assert lag < timedelta(minutes=5), \
            f"Pipeline çok geride! Gecikme: {lag}"
        print(f"  Pipeline gecikmesi: {lag}")
    cur.close()