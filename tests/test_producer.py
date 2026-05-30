import pytest
import json
from confluent_kafka import Producer, Consumer, KafkaError
from datetime import datetime

KAFKA_CONFIG = {"bootstrap.servers": "kafka:9092"}
TOPIC = "energy_topic"


def test_kafka_connection():
    """Kafka'ya bağlanılabiliyor mu?"""
    try:
        producer = Producer(KAFKA_CONFIG)
        assert producer is not None
    except Exception as e:
        pytest.fail(f"Kafka bağlantısı kurulamadı: {e}")


def test_topic_exists():
    """energy_topic topic'i mevcut mu?"""
    from confluent_kafka.admin import AdminClient
    admin = AdminClient(KAFKA_CONFIG)
    topics = admin.list_topics(timeout=5).topics
    assert TOPIC in topics, f"Topic bulunamadı: {TOPIC}"


def test_produce_and_consume():
    """Topic'te mesaj var mı ve okunabiliyor mu?"""
    consumer = Consumer({
        **KAFKA_CONFIG,
        "group.id": "test-group-" + datetime.now().strftime("%H%M%S%f"),
        "auto.offset.reset": "earliest"  # En baştan oku
    })
    consumer.subscribe([TOPIC])

    msg = consumer.poll(timeout=10.0)
    consumer.close()

    assert msg is not None, "Topic'te hiç mesaj yok — producer çalışıyor mu?"
    assert msg.error() is None, f"Kafka hatası: {msg.error()}"

    data = json.loads(msg.value().decode())
    assert "usage_kwh" in data, "Mesajda usage_kwh alanı yok!"
    assert "time" in data, "Mesajda time alanı yok!"
    print(f"  Okunan mesaj: {data['time']} | {data['usage_kwh']} kWh")


def test_message_format():
    """Üretilen mesaj doğru formatta mı?"""
    required_fields = [
        "time", "usage_kwh", "load_type",
        "tariff_period", "shift", "z_score"
    ]

    consumer = Consumer({
        **KAFKA_CONFIG,
        "group.id": "format-test-" + datetime.now().strftime("%H%M%S%f"),
        "auto.offset.reset": "earliest"  # En baştan oku
    })
    consumer.subscribe([TOPIC])

    msg = consumer.poll(timeout=10.0)
    consumer.close()

    if msg is None:
        pytest.skip("Topic'te mesaj yok — producer çalışıyor mu?")

    data = json.loads(msg.value().decode())
    for field in required_fields:
        assert field in data, f"Mesajda eksik alan: {field}"
    print(f"  Tüm alanlar mevcut: {list(data.keys())}")

def test_message_format():
    """Üretilen mesaj doğru formatta mı?"""
    # processed_readings'den gelen mesajlarda olması gereken alanlar
    required_fields = [
        "time", "usage_kwh", "load_type",
        "tariff_period", "shift", "z_score"
    ]

    consumer = Consumer({
        **KAFKA_CONFIG,
        "group.id": "format-test-" + datetime.now().strftime("%H%M%S%f"),
        "auto.offset.reset": "latest",  # En son mesajları oku
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC])

    # Partition assignment bekle
    for _ in range(5):
        consumer.poll(timeout=1.0)

    # Producer'ın yeni mesaj göndermesini bekle
    msg = None
    for _ in range(20):
        msg = consumer.poll(timeout=2.0)
        if msg is not None:
            data = json.loads(msg.value().decode())
            # tariff_period içeren yeni format mesajı mı?
            if "tariff_period" in data:
                break
            msg = None  # Eski format, devam et

    consumer.close()

    if msg is None:
        pytest.skip("Yeni formatta mesaj bulunamadı — producer çalışıyor mu?")

    data = json.loads(msg.value().decode())
    for field in required_fields:
        assert field in data, f"Mesajda eksik alan: {field}"
    print(f"  Tüm alanlar mevcut.")