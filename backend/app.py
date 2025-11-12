from flask import Flask, jsonify
from google.cloud import bigquery
from flask_cors import CORS
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize BigQuery client
key_path = os.getenv("BIGQUERY_KEY_JSON")
client = bigquery.Client.from_service_account_json(key_path)

app = Flask(__name__)
CORS(app)

#Get all transactions
@app.route("/api/transactions")
def all_transactions():
    query = """
    SELECT * 
    FROM `njc-ezpass.ezpass_data.gold`
    ORDER BY transaction_date DESC
    """
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    return jsonify({"data": rows})

#Get flagged or investigating transactions (Recent Alerts)
@app.route("/api/transactions/alerts")
def alerts():
    query = f"SELECT * FROM `njc-ezpass.ezpass_data.gold` WHERE flag_fraud = TRUE OR threat_severity IS NOT NULL LIMIT 100"
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    return jsonify({"data": rows})

#Aggregated metrics for dashboard cards
@app.route("/api/metrics")
def metrics():
    query = f"""
    SELECT
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN flag_fraud THEN 1 ELSE 0 END) AS total_flagged,
        SUM(amount) AS total_amount
    FROM `njc-ezpass.ezpass_data.gold`
    """
    results = client.query(query).result()
    metrics = dict(next(results))
    return jsonify(metrics)

#Fraud by Category for chart
@app.route("/api/charts/category")
def category_chart():
    query = """
    SELECT 
        'Holiday' AS category,
        COUNT(*) AS count
    FROM `njc-ezpass.ezpass_data.gold`
    WHERE flag_fraud = TRUE AND flag_is_holiday = TRUE
    UNION ALL
    SELECT 
        'Out of State' AS category,
        COUNT(*) AS count
    FROM `njc-ezpass.ezpass_data.gold`
    WHERE flag_fraud = TRUE AND flag_is_out_of_state = TRUE
    UNION ALL
    SELECT 
        'Vehicle Type > 2' AS category,
        COUNT(*) AS count
    FROM `njc-ezpass.ezpass_data.gold`
    WHERE flag_fraud = TRUE AND flag_is_vehicle_type_gt2 = TRUE
    UNION ALL
    SELECT 
        'Weekend' AS category,
        COUNT(*) AS count
    FROM `njc-ezpass.ezpass_data.gold`
    WHERE flag_fraud = TRUE AND flag_is_weekend = TRUE
    ORDER BY count DESC
    """
    try:
        results = client.query(query).result()
        data = [{"category": row["category"], "count": int(row["count"])} for row in results]
        # Filter out categories with 0 count
        data = [item for item in data if item["count"] > 0]
        return jsonify({"data": data})
    except Exception as e:
        print(f"Error fetching category chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

#Threat Severity for chart
@app.route("/api/charts/severity")
def severity_chart():
    query = f"""
    SELECT threat_severity, COUNT(*) AS count
    FROM `njc-ezpass.ezpass_data.gold`
    WHERE threat_severity IS NOT NULL
    GROUP BY threat_severity
    """
    results = client.query(query).result()
    data = [{"severity": row["threat_severity"], "count": row["count"]} for row in results]
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
            SUM(CASE WHEN flag_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_alerts
        FROM `njc-ezpass.ezpass_data.gold`
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

if __name__ == "__main__":
    app.run(debug=True)
