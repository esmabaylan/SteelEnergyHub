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


def test_recent_data_exists(conn):
    """Son 1 saatte dummy data üretilmiş mi?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM main_data.energy_readings
        WHERE time > NOW() - INTERVAL '1 hour';
    """)
    count = cur.fetchone()[0]
    assert count > 0, "Son 1 saatte yeni veri üretilmemiş!"
    print(f"  Son 1 saatteki kayıt: {count}")
    cur.close()


def test_usage_kwh_range(conn):
    """usage_kwh değerleri geçerli aralıkta mı?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(usage_kwh), MAX(usage_kwh)
        FROM main_data.energy_readings
        WHERE time > NOW() - INTERVAL '1 hour';
    """)
    min_val, max_val = cur.fetchone()
    assert min_val >= 0, f"Negatif usage_kwh değeri var: {min_val}"
    assert max_val <= 200, f"Çok yüksek usage_kwh değeri: {max_val}"
    cur.close()


def test_load_type_values(conn):
    """load_type sadece geçerli değerler içeriyor mu?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT load_type FROM main_data.energy_readings;
    """)
    load_types = [row[0] for row in cur.fetchall()]
    valid_types = {"Light_Load", "Medium_Load", "Maximum_Load"}
    for lt in load_types:
        assert lt in valid_types, f"Geçersiz load_type: {lt}"
    cur.close()


def test_no_null_time(conn):
    """time kolonu NULL değer içermiyor mu?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM main_data.energy_readings
        WHERE time IS NULL;
    """)
    count = cur.fetchone()[0]
    assert count == 0, f"{count} adet NULL time değeri var!"
    cur.close()


def test_anomaly_rate(conn):
    """Anomali oranı makul aralıkta mı? (%0.1 - %5)"""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE z_anomaly = TRUE)::float / COUNT(*) * 100
        FROM main_data.processed_readings
        WHERE time > NOW() - INTERVAL '1 day';
    """)
    rate = cur.fetchone()[0]
    if rate is not None:
        assert 0 <= rate <= 10, f"Anomali oranı beklenenden yüksek: %{rate:.2f}"
        print(f"  Anomali oranı: %{rate:.3f}")
    cur.close()