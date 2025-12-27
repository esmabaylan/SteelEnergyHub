# Test DB conncections and queries
import pytest
import psycopg2

def test_db_connection():
    """Test database connection."""
    try:
        conn = psycopg2.connect(
            dbname="energydb",
            user="postgres",
            password="postgres",
            host="host.docker.internal",
            port="5435"
           # host="localhost",
           # port="5432"
        )
        return conn
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")


def test_db_creation():
    """Test database creation."""
    try:
        conn = test_db_connection()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS energy_metrics (id SERIAL PRIMARY KEY, metric_name VARCHAR(50), metric_value FLOAT);")
        conn.commit()
        print("Database table creation successful.")
        conn.close()
    except Exception as e:
        pytest.fail(f"Database table creation failed: {e}")

def test_db_query():
    """Test database query."""
    try:
        conn = test_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM energy_metrics;")
        print(cursor.fetchall())
        print("Database query successful.")
        conn.close()
    except Exception as e:
        pytest.fail(f"Database query failed: {e}")

if __name__ == "__main__":
    test_db_creation()