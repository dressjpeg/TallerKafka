from kafka import KafkaProducer
import pandas as pd
import json
import time

# Configurar productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Leer dataset
df = pd.read_csv('../data/heart.csv')

print("Enviando registros a Kafka...")

# Enviar fila por fila
for index, row in df.iterrows():

    # Convertir fila a diccionario
    message = row.to_dict()

    # Enviar al topic
    producer.send('heart-records', value=message)

    print(f"Registro enviado: {message}")

    # Simular streaming en tiempo real
    time.sleep(2)

print("Todos los registros fueron enviados.")