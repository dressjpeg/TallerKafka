Requisitos

Docker Desktop

Python 3.11

MongoDB

Power BI Desktop

Java 17

Hadoop Winutils



Flujo del proyecto

El productor Kafka (producer.py) lee el dataset CSV y envía cada paciente al topic heart-records.

Spark (consumer.py) consume los datos en tiempo real desde Kafka.

Se aplica el modelo de Machine Learning entrenado para predecir riesgo cardíaco.

Los resultados se almacenan en MongoDB.

Flask API expone endpoints para consultar la información.

Power BI consume la API y genera el dashboard.



Levantar Docker

docker compose up -d

2\. Ejecutar el productor Kafka

cd producer

python producer.py

3\. Ejecutar Spark Consumer

cd spark

python consumer.py

4\. Ejecutar Flask API

cd flask\_api

python app.py



La API quedará disponible en:



http://127.0.0.1:5000

Endpoints disponibles

/pacientes

/predicciones

/estadisticas

/resumen-riesgo

