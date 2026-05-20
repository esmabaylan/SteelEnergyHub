from kafka import KafkaProducer
import json
import time
import pandas as pd

# Eğer Docker dışından çalıştırıyorsan localhost:9092, 
# Docker içindeki başka bir konteynerden çalıştırıyorsan kafka:9092 kullan.
producer=KafkaProducer(bootstrap_servers=['kafka:9092'],
              api_version=(0,11,5),
              value_serializer=lambda x:json.dumps(x).encode('utf-8'))
# producer = KafkaProducer(
    
#     bootstrap_servers=['localhost:9092'],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )


df=pd.read_csv('/home/jovyan/work/data/raw/Steel_industry_data.csv')
print("Veri yüklendi, Kafka'ya gönderiliyor...")
for index, row in df.iterrows():
    data = row.to_dict()
    producer.send('energydata', value=data)
    print(f"Gönderildi: {data}")
    time.sleep(0.5)

producer.flush()