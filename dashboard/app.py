from flask import Flask, render_template_string
from pymongo import MongoClient
import pandas as pd

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["kafkamed"]
collection = db["patients"]

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
    <h1>{{ risk }}</h1>
</div>

<div class="card">
    <h2>Pacientes sin riesgo</h2>
    <h1>{{ no_risk }}</h1>
</div>

</body>
</html>
"""

@app.route("/")
def home():

    data = list(collection.find())

    df = pd.DataFrame(data)

    if len(df) == 0:
        total = 0
        risk = 0
        no_risk = 0
    else:
        total = len(df)
        risk = len(df[df["prediction"] == 1.0])
        no_risk = len(df[df["prediction"] == 0.0])

    return render_template_string(
        HTML,
        total=total,
        risk=risk,
        no_risk=no_risk
    )

if __name__ == "__main__":
    app.run(debug=True)