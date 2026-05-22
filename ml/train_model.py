from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# SPARK SESSION
spark = SparkSession.builder \
    .appName("HeartModelTraining") \
    .getOrCreate()

# LEER DATASET
df = spark.read.csv(
    "../data/heart.csv",
    header=True,
    inferSchema=True
)

print("Dataset cargado correctamente")
df.show(5)

# COLUMNAS CATEGÓRICAS
categorical_cols = [
    "Sex",
    "ChestPainType",
    "RestingECG",
    "ExerciseAngina",
    "ST_Slope"
]

# STRING INDEXERS
indexers = [
    StringIndexer(
        inputCol=col,
        outputCol=col + "_index"
    )
    for col in categorical_cols
]

# FEATURES DEL MODELO
feature_cols = [
    "Age",
    "Sex_index",
    "ChestPainType_index",
    "RestingBP",
    "Cholesterol",
    "FastingBS",
    "RestingECG_index",
    "MaxHR",
    "ExerciseAngina_index",
    "Oldpeak",
    "ST_Slope_index"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

# RANDOM FOREST
rf = RandomForestClassifier(
    labelCol="HeartDisease",
    featuresCol="features",
    numTrees=20
)

# PIPELINE ML
pipeline = Pipeline(
    stages=indexers + [assembler, rf]
)

# ENTRENAR MODELO
print("Entrenando modelo...")
model = pipeline.fit(df)

# GUARDAR MODELO
model.write().overwrite().save("./heart_model")
print("Modelo entrenado y guardado correctamente")