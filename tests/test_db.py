import pytest
import psycopg2

@pytest.fixture
def db_connection():
    conn = psycopg2.connect(
        dbname="energydb",
        user="postgres",
        password="postgres",
        host="host.docker.internal", 
        port="5435"
    )
    

    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS energy_metrics;")
    cursor.execute("""
        CREATE TABLE energy_metrics (
            id SERIAL PRIMARY KEY, 
            metric_name VARCHAR(50), 
            metric_value FLOAT
        );
    """)
    conn.commit()

    # 2. MENTÖRLÜK: yield, bağlantıyı teste verir, test bitince alt satırdan çalışmaya devam eder.
    yield conn  

    # Temizlik (Teardown): Test bittikten sonra bağlantıyı kapatıyoruz.
    conn.close()


# 3. MENTÖRLÜK: Test fonksiyonları 'test_' ile başlar ve bağımsız (izole) olmalıdır.
def test_db_insertion_and_query(db_connection):
    """Veri ekleme ve o veriyi çekme işlemini test eder."""
    
    # db_connection fixture'ı otomatik olarak bu teste parametre olarak geldi.
    cursor = db_connection.cursor()

    # Eylem (Act): Veritabanına test verisi ekle
    test_metric_name = "cpu_usage"
    test_metric_value = 85.5
    cursor.execute(
        "INSERT INTO energy_metrics (metric_name, metric_value) VALUES (%s, %s);", 
        (test_metric_name, test_metric_value)
    )
    db_connection.commit()

    # Eylem (Act): Eklenen veriyi geri çek
    cursor.execute("SELECT metric_name, metric_value FROM energy_metrics WHERE metric_name = %s;", (test_metric_name,))
    result = cursor.fetchone()

    # 4. MENTÖRLÜK: Assert kullanımı. Pytest'in kalbi burasıdır.
    # Burada try-except kullanmıyoruz. Eğer veri dönmezse assert None'da patlar ve hatayı görürüz.
    assert result is not None, "Veritabanından sorgulanan kayıt dönmedi!"
    assert result[0] == test_metric_name, f"Beklenen metrik ismi '{test_metric_name}', ancak '{result[0]}' geldi."
    assert result[1] == test_metric_value, "Veritabanına yazılan sayısal değer bozulmuş veya yanlış gelmiş."