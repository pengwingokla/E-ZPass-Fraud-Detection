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
    FROM `njc-ezpass.ezpass_data.gold_automation`
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
    FROM `njc-ezpass.ezpass_data.gold_automation` 
    WHERE flag_fraud = TRUE OR threat_severity IS NOT NULL 
    ORDER BY transaction_date DESC
    LIMIT 100
    """
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    return jsonify({"data": rows})

#Aggregated metrics for dashboard cards
@app.route("/api/metrics")
def metrics():
    query = """
    SELECT
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN flag_fraud = TRUE THEN 1 ELSE 0 END) AS total_flagged,
        SUM(amount) AS total_amount,
        SUM(CASE WHEN flag_fraud = TRUE AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN 1 ELSE 0 END) AS total_alerts_ytd,
        SUM(CASE WHEN flag_fraud = TRUE AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) 
                 AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN 1 ELSE 0 END) AS detected_frauds_current_month
    FROM `njc-ezpass.ezpass_data.gold_automation`
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
    WITH fraud_transactions AS (
        SELECT 
            triggered_flags
        FROM `njc-ezpass.ezpass_data.gold_automation`
        WHERE flag_fraud = TRUE AND triggered_flags IS NOT NULL AND triggered_flags != ''
    ),
    split_flags AS (
        SELECT 
            TRIM(REPLACE(REPLACE(REPLACE(REPLACE(flag, '"', ''), '[', ''), ']', ''), ' ', '')) AS category
        FROM fraud_transactions,
        UNNEST(SPLIT(triggered_flags, ',')) AS flag
        WHERE TRIM(flag) != '' AND TRIM(flag) != 'null'
    )
    SELECT 
        category,
        COUNT(*) AS count
    FROM split_flags
    WHERE category IS NOT NULL AND category != ''
    GROUP BY category
    HAVING COUNT(*) > 0
    ORDER BY count DESC
    """
    try:
        results = client.query(query).result()
        data = []
        for row in results:
            category = str(row["category"]).strip() if row["category"] else None
            if category and category.lower() not in ['null', 'none', '']:
                data.append({"category": category, "count": int(row["count"])})
        return jsonify({"data": data})
    except Exception as e:
        print(f"Error fetching category chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

#Threat Severity for chart
@app.route("/api/charts/severity")
def severity_chart():
    query = """
    SELECT threat_severity, COUNT(*) AS count
    FROM `njc-ezpass.ezpass_data.gold_automation`
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
        FROM `njc-ezpass.ezpass_data.gold_automation`
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
