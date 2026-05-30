import pytest
import psycopg2

DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}

TABLES = [
    "main_data.energy_readings",
    "main_data.processed_readings",
    "main_data.anomalies",
    "main_data.cost_analysis"
]


@pytest.fixture
def conn():
    connection = psycopg2.connect(**DB_CONFIG)
    yield connection
    connection.close()


def test_connection(conn):
    """Veritabanı bağlantısı kurulabiliyor mu?"""
    assert conn is not None


def test_tables_exist(conn):
    """Tüm tablolar mevcut mu?"""
    cur = conn.cursor()
    for table in TABLES:
        schema, name = table.split(".")
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """, (schema, name))
        exists = cur.fetchone()[0]
        assert exists, f"Tablo bulunamadı: {table}"
    cur.close()


def test_energy_readings_has_data(conn):
    """energy_readings tablosunda veri var mı?"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM main_data.energy_readings;")
    count = cur.fetchone()[0]
    assert count > 0, "energy_readings tablosu boş!"
    print(f"  energy_readings satır sayısı: {count}")
    cur.close()


def test_processed_readings_has_data(conn):
    """processed_readings tablosunda veri var mı?"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM main_data.processed_readings;")
    count = cur.fetchone()[0]
    assert count > 0, "processed_readings tablosu boş!"
    print(f"  processed_readings satır sayısı: {count}")
    cur.close()


def test_anomalies_has_data(conn):
    """anomalies tablosunda veri var mı?"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM main_data.anomalies;")
    count = cur.fetchone()[0]
    assert count > 0, "anomalies tablosu boş!"
    print(f"  anomalies satır sayısı: {count}")
    cur.close()


def test_roles_exist(conn):
    """data_writer ve report_reader rolleri var mı?"""
    cur = conn.cursor()
    for role in ["data_writer", "report_reader"]:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_catalog.pg_roles WHERE rolname = %s
            );
        """, (role,))
        exists = cur.fetchone()[0]
        assert exists, f"Rol bulunamadı: {role}"
    cur.close()


def test_hypertables(conn):
    """Tablolar hypertable olarak tanımlı mı?"""
    cur = conn.cursor()
    cur.execute("""
        SELECT hypertable_name
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'main_data';
    """)
    hypertables = [row[0] for row in cur.fetchall()]
    for table in ["energy_readings", "processed_readings", "anomalies", "cost_analysis"]:
        assert table in hypertables, f"{table} hypertable değil!"
    cur.close()