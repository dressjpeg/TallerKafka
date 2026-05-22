from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pymongo import MongoClient
from datetime import datetime
from pyspark.ml import PipelineModel

# SESIÓN SPARK

spark = SparkSession.builder \
    .appName("KafkaHeartConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
model = PipelineModel.load("../ml/heart_model")

# MONGODB

client = MongoClient("mongodb://localhost:27017/")
db = client["kafkamed"]
collection = db["patients"]


# ESQUEMA JSON

schema = StructType([
    StructField("Age", IntegerType(), True),
    StructField("Sex", StringType(), True),
    StructField("ChestPainType", StringType(), True),
    StructField("RestingBP", IntegerType(), True),
    StructField("Cholesterol", IntegerType(), True),
    StructField("FastingBS", IntegerType(), True),
    StructField("RestingECG", StringType(), True),
    StructField("MaxHR", IntegerType(), True),
    StructField("ExerciseAngina", StringType(), True),
    StructField("Oldpeak", DoubleType(), True),
    StructField("ST_Slope", StringType(), True),
    StructField("HeartDisease", IntegerType(), True)
])

# LEER DESDE KAFKA

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "heart-records") \
    .option("failOnDataLoss", "false") \
    .load()

# CONVERTIR JSON

df_string = df_raw.selectExpr("CAST(value AS STRING)")
df_json = df_string.select(
    from_json(col("value"), schema).alias("data")
)


# EXPANDIR COLUMNAS

df_final = df_json.select("data.*")
predictions = model.transform(df_final)

result_df = predictions.select(
    "Age",
    "Sex",
    "ChestPainType",
    "RestingBP",
    "Cholesterol",
    "MaxHR",
    "prediction",
    "probability"
)

# GUARDAR EN MONGODB

def save_to_mongo(batch_df, batch_id):
    rows = batch_df.collect()
    documents = []

    for row in rows:
        doc = row.asDict()
        # Convertir DenseVector a lista normal
        if "probability" in doc:
            doc["probability"] = doc["probability"].toArray().tolist()
        doc["timestamp"] = datetime.now()
        documents.append(doc)

    if documents:
        collection.insert_many(documents)
        print(f"\nBatch {batch_id} guardado en MongoDB")
        print(f"Documentos insertados: {len(documents)}")

# STREAMING

query = result_df.writeStream \
    .outputMode("append") \
    .foreachBatch(save_to_mongo) \
    .option("checkpointLocation", "C:/spark-checkpoint") \
    .start()

query.awaitTermination()