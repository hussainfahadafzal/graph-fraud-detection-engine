from flask import Flask, render_template, request
import os
import pandas as pd
from detection.graph_analysis import analyze_graph

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"

os.makedirs("uploads", exist_ok=True)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return "No file uploaded"

    file = request.files["file"]

    if file.filename == "":
        return "No selected file"

    filepath = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
    file.save(filepath)

    df = pd.read_csv(filepath)

    result = analyze_graph(df)

    return render_template("result.html", data=result)

if __name__ == "__main__":
    app.run(debug=True)
