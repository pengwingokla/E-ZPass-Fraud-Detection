from flask import Flask, jsonify
from google.cloud import bigquery
from flask_cors import CORS
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize BigQuery client
key_path = os.getenv("BIGQUERY_KEY_JSON")
if not key_path:
    raise ValueError(
        "BIGQUERY_KEY_JSON environment variable is not set. "
        "Please set it in your .env file with the path to your service account JSON key."
    )

# Resolve relative paths relative to project root (parent of backend directory)
if not os.path.isabs(key_path):
    # Get the project root (parent of backend directory)
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(backend_dir)
    # Remove leading ./ if present
    key_path = key_path.lstrip('./')
    key_path = os.path.join(project_root, key_path)

if not os.path.exists(key_path):
    raise FileNotFoundError(
        f"BigQuery service account key file not found at: {key_path}. "
        "Please verify the path in your .env file."
    )

try:
    client = bigquery.Client.from_service_account_json(key_path)
except Exception as e:
    raise RuntimeError(
        f"Failed to initialize BigQuery client: {str(e)}. "
        "Please verify your service account key is valid and has the required permissions."
    )

app = Flask(__name__)
CORS(app)

#Get all transactions
@app.route("/api/transactions")
def all_transactions():
    query = """
    SELECT * 
    FROM `njc-ezpass.ezpass_data.master_viz`
    ORDER BY transaction_date DESC
    """
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    return jsonify({"data": rows})

#Get flagged or investigating transactions (Recent Alerts)
@app.route("/api/transactions/alerts")
def alerts():
    query = """
    SELECT * 
    FROM `njc-ezpass.ezpass_data.master_viz` 
    WHERE is_anomaly = 1
    ORDER BY transaction_date DESC
    LIMIT 100
    """
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    return jsonify({"data": rows})

#Get recent flagged transactions for homepage card
@app.route("/api/transactions/recent-flagged")
def recent_flagged():
    query = """
    SELECT 
        transaction_id,
        transaction_date,
        tag_plate_number,
        agency,
        amount,
        status,
        ml_predicted_category,
        is_anomaly
    FROM `njc-ezpass.ezpass_data.master_viz` 
    WHERE (status = 'Needs Review' OR is_anomaly = 1)
    ORDER BY transaction_date DESC
    LIMIT 3
    """
    try:
        results = client.query(query).result()
        rows = []
        for row in results:
            # Use actual status from database
            status = row.get('status')
            
            # Map ml_predicted_category to category, with fallback
            category = row.get('ml_predicted_category') or 'Anomaly Detected'
            
            rows.append({
                'id': row.get('transaction_id'),
                'transaction_id': row.get('transaction_id'),
                'transaction_date': str(row.get('transaction_date')) if row.get('transaction_date') else None,
                'tag_plate_number': row.get('tag_plate_number'),
                'tagPlate': row.get('tag_plate_number'),  # Also include tagPlate for compatibility
                'agency': row.get('agency'),
                'amount': float(row.get('amount')) if row.get('amount') is not None else 0.0,
                'status': status,
                'category': category,
                'is_anomaly': bool(row.get('is_anomaly'))
            })
        return jsonify({"data": rows})
    except Exception as e:
        print(f"Error fetching recent flagged transactions: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

#Aggregated metrics for dashboard cards
@app.route("/api/metrics")
def metrics():
    query = """
    SELECT
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS total_flagged,
        SUM(CASE WHEN is_anomaly = 1 THEN amount ELSE 0 END) AS total_amount,
        SUM(CASE WHEN is_anomaly = 1 AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN 1 ELSE 0 END) AS total_alerts_ytd,
        SUM(CASE WHEN is_anomaly = 1 AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) 
                 AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN 1 ELSE 0 END) AS detected_frauds_current_month
    FROM `njc-ezpass.ezpass_data.master_viz`
    """
    try:
        results = client.query(query).result()
        metrics = dict(next(results))
        return jsonify({
            "total_transactions": int(metrics.get("total_transactions", 0)),
            "total_flagged": int(metrics.get("total_flagged", 0)),
            "total_amount": float(metrics.get("total_amount", 0)),
            "total_alerts_ytd": int(metrics.get("total_alerts_ytd", 0)),
            "detected_frauds_current_month": int(metrics.get("detected_frauds_current_month", 0))
        })
    except Exception as e:
        print(f"Error fetching metrics: {str(e)}")
        return jsonify({
            "total_transactions": 0,
            "total_flagged": 0,
            "total_amount": 0,
            "total_alerts_ytd": 0,
            "detected_frauds_current_month": 0
        }), 500

#Fraud by Category for chart
@app.route("/api/charts/category")
def category_chart():
    query = """
        WITH unpivoted AS (
            SELECT
                f.flag_label AS category
            FROM `njc-ezpass.ezpass_data.master_viz`,
            UNNEST([
                STRUCT('Rush Hour' AS flag_label, flag_rush_hour AS flag_value),
                STRUCT('Weekend' AS flag_label, flag_is_weekend AS flag_value),
                STRUCT('Holiday' AS flag_label, flag_is_holiday AS flag_value),
                STRUCT('Overlapping Journey' AS flag_label, flag_overlapping_journey AS flag_value),
                STRUCT('Driver Amount Outlier' AS flag_label, flag_driver_amount_outlier AS flag_value),
                STRUCT('Route Amount Outlier' AS flag_label, flag_route_amount_outlier AS flag_value),
                STRUCT('Amount Unusually High' AS flag_label, flag_amount_unusually_high AS flag_value),
                STRUCT('Driver Spend Spike' AS flag_label, flag_driver_spend_spike AS flag_value)
            ]) AS f
            WHERE is_anomaly = 1
                AND f.flag_value IS TRUE
            )
            SELECT category, COUNT(*) AS count
            FROM unpivoted
            GROUP BY category
            ORDER BY count DESC;

    """
    try:
        results = client.query(query).result()
        data = [{"category": row["category"], "count": row["count"]} for row in results]
        return jsonify({"data": data})
    except Exception as e:
        print("BACKEND ERROR:", e)
        return jsonify({"error": str(e), "data": []}), 500


#Threat Severity for chart
@app.route("/api/charts/severity")
def severity_chart():
    query = """
    SELECT ml_predicted_category AS severity, COUNT(*) AS count
    FROM `njc-ezpass.ezpass_data.master_viz`
    WHERE ml_predicted_category IS NOT NULL
    GROUP BY ml_predicted_category
    """
    results = client.query(query).result()

    data = [
        {"severity": row["severity"], "count": row["count"]}
        for row in results
    ]

    return jsonify({"data": data})


#Monthly transaction analysis for bar chart
@app.route("/api/charts/monthly")
def monthly_chart():
    try:
        query = """
        SELECT 
            FORMAT_DATE('%b %Y', DATE(transaction_date)) AS month,
            EXTRACT(YEAR FROM DATE(transaction_date)) AS year,
            EXTRACT(MONTH FROM DATE(transaction_date)) AS month_num,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS fraud_alerts
        FROM `njc-ezpass.ezpass_data.master_viz`
        WHERE transaction_date IS NOT NULL
        GROUP BY year, month_num, month
        ORDER BY year DESC, month_num DESC
        LIMIT 12
        """
        results = client.query(query).result()
        data = [{
            "month": row["month"],
            "year": int(row["year"]),
            "month_num": int(row["month_num"]),
            "total_transactions": int(row["total_transactions"]),
            "fraud_alerts": int(row["fraud_alerts"] or 0)
        } for row in results]
        # Reverse to show oldest to newest (or keep newest first)
        data.reverse()
        return jsonify({"data": data})
    except Exception as e:
        print(f"Error fetching monthly chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

#Scatter plot data for ml_anomaly_score vs amount
@app.route("/api/charts/scatter")
def scatter_chart():
    try:
        query = """
        SELECT 
            amount,
            ml_predicted_score AS ml_anomaly_score,
            ml_predicted_category AS risk_level
        FROM `njc-ezpass.ezpass_data.master_viz`
        WHERE DATE(transaction_date) >= '2024-01-01'
            AND amount IS NOT NULL
            AND ml_predicted_score IS NOT NULL
            AND ml_predicted_category IS NOT NULL
        """
        results = client.query(query).result()
        data = [{
            "amount": float(row["amount"]) if row["amount"] is not None else None,
            "ml_anomaly_score": float(row["ml_anomaly_score"]) if row["ml_anomaly_score"] is not None else None,
            "risk_level": row["risk_level"]
        } for row in results]
        return jsonify({"data": data})
    except Exception as e:
        print(f"Error fetching scatter chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

#Time series data for anomaly counts by hour
@app.route("/api/charts/timeseries")
def timeseries_chart():
    try:
        query = """
        SELECT 
            EXTRACT(HOUR FROM entry_time) AS hour,
            COUNT(*) AS fraud_count
        FROM `njc-ezpass.ezpass_data.master_viz`
        WHERE is_anomaly = 1
        GROUP BY hour
        ORDER BY hour
        """
        results = client.query(query).result()
        data = [{
            "hour": int(row["hour"]),
            "fraud_count": int(row["fraud_count"])
        } for row in results]
        return jsonify({"data": data})
    except Exception as e:
        print(f"Error fetching timeseries chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5001)
