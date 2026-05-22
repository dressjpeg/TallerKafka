from flask import Flask, jsonify
from pymongo import MongoClient
from bson import json_util
import json

app = Flask(__name__)

# MONGODB
client = MongoClient("mongodb://localhost:27017/")
db = client["kafkamed"]
collection = db["patients"]

# INICIO
@app.route("/")
def inicio():
    return "API KafkaMed funcionando"

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

if __name__ == "__main__":
    app.run(debug=True)