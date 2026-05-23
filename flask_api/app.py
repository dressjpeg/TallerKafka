from flask import Flask, jsonify, render_template_string
from pymongo import MongoClient
from bson import json_util
import json
import pandas as pd

app = Flask(__name__)

# CONEXIÓN MONGODB

client = MongoClient("mongodb://localhost:27017/")
db = client["kafkamed"]
collection = db["patients"]

# HTML DASHBOARD

HTML = """
<!DOCTYPE html>
<html>

<head>

    <title>KafkaMed Dashboard</title>

    <meta http-equiv="refresh" content="5">

    <style>

        body {
            font-family: Arial;
            margin: 40px;
            background-color: #f4f4f4;
        }

        .card {
            background: white;
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 10px;
        }

        h1 {
            color: #333;
        }

    </style>

</head>

<body>

<h1>KafkaMed - Dashboard en Tiempo Real</h1>

<div class="card">
    <h2>Total pacientes procesados</h2>
    <h1>{{ total }}</h1>
</div>

<div class="card">
    <h2>Pacientes con riesgo cardíaco</h2>
    <h1>{{ riesgo }}</h1>
</div>

<div class="card">
    <h2>Pacientes sin riesgo</h2>
    <h1>{{ sin_riesgo }}</h1>
</div>

</body>

</html>
"""

# DASHBOARD PRINCIPAL

@app.route("/")
def dashboard():

    data = list(collection.find())

    df = pd.DataFrame(data)

    if len(df) == 0:
        total = 0
        riesgo = 0
        sin_riesgo = 0

    else:
        total = len(df)
        riesgo = len(df[df["prediction"] == 1.0])
        sin_riesgo = len(df[df["prediction"] == 0.0])

    return render_template_string(
        HTML,
        total=total,
        riesgo=riesgo,
        sin_riesgo=sin_riesgo
    )

# TODOS LOS PACIENTES

@app.route("/pacientes")
def pacientes():

    data = list(collection.find({}, {"_id": 0}).limit(100))

    return json.loads(json_util.dumps(data))

# SOLO PREDICCIONES

@app.route("/predicciones")
def predicciones():

    data = list(collection.find(
        {},
        {
            "_id": 0,
            "Age": 1,
            "prediction": 1,
            "probability": 1
        }
    ).limit(100))

    return json.loads(json_util.dumps(data))

# PACIENTES EN RIESGO

@app.route("/riesgo")
def riesgo():

    data = list(collection.find(
        {"prediction": 1},
        {"_id": 0}
    ).limit(100))

    return json.loads(json_util.dumps(data))

# ESTADÍSTICAS

@app.route("/estadisticas")
def estadisticas():

    total = collection.count_documents({})

    riesgo = collection.count_documents({"prediction": 1})

    sin_riesgo = collection.count_documents({"prediction": 0})

    return jsonify({
        "total_pacientes": total,
        "con_riesgo": riesgo,
        "sin_riesgo": sin_riesgo
    })

# EJECUTAR

if __name__ == "__main__":
    app.run(debug=True)