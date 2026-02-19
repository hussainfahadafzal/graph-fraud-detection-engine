from flask import Flask, render_template, request, jsonify
import os
import pandas as pd
from werkzeug.utils import secure_filename

from detection.graph_analysis import analyze_transactions


app = Flask(__name__)

# Optional upload directory (not required for analysis logic)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10 MB upload limit

REQUIRED_COLUMNS = [
    "transaction_id",
    "sender_id",
    "receiver_id",
    "amount",
    "timestamp",
]


@app.route("/")
def index():
    return render_template("index.html")


def _error_response(message: str, code: int = 400, details: dict | None = None):
    payload = {
        "status": "error",
        "message": message,
    }
    if details:
        payload["details"] = details
    return jsonify(payload), code


@app.post("/analyze")
def analyze():
    """
    Accept a CSV upload, validate schema, run analysis, and return JSON.
    """
    if "file" not in request.files:
        return _error_response("No file part in request.", 400, {"code": "NO_FILE_PART"})

    file = request.files["file"]

    if not file or file.filename == "":
        return _error_response("No file selected. Please choose a CSV file.", 400, {"code": "NO_FILE"})

    filename = secure_filename(file.filename)
    if not filename.lower().endswith(".csv"):
        return _error_response(
            "Invalid file type. Please upload a CSV file.",
            400,
            {"code": "INVALID_FILE_TYPE"},
        )

    try:
        # Read directly from the in-memory file object to avoid path dependencies
        df = pd.read_csv(file)
    except Exception as exc:  # pragma: no cover - defensive
        return _error_response(
            "Unable to read CSV file. Please verify the file is a valid, comma-separated CSV.",
            400,
            {"code": "PARSE_ERROR", "details": str(exc)},
        )

    # Schema validation
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return _error_response(
            "CSV is missing required columns.",
            400,
            {
                "code": "MISSING_COLUMNS",
                "required_columns": REQUIRED_COLUMNS,
                "missing_columns": missing,
                "received_columns": list(df.columns),
            },
        )

    # Re-order and drop extra columns so downstream logic has a clean schema
    df = df[REQUIRED_COLUMNS]

    try:
        result = analyze_transactions(df, max_runtime_seconds=30.0)
    except ValueError as ve:
        # Validation-style errors from the analysis layer
        return _error_response(
            "CSV failed validation checks.",
            400,
            {"code": "VALIDATION_ERROR", "details": str(ve)},
        )
    except Exception as exc:  # pragma: no cover - defensive
        # Unexpected server-side error
        return _error_response(
            "An unexpected error occurred while analyzing the graph.",
            500,
            {"code": "INTERNAL_ERROR", "details": str(exc)},
        )

    return jsonify(
        {
            "status": "ok",
            "data": result,
        }
    )


if __name__ == "__main__":
    # When run via gunicorn (production), this block is not executed.
    app.run(debug=True)
