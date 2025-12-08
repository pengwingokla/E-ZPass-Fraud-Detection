from flask import Flask, jsonify, request
from google.cloud import bigquery
from flask_cors import CORS
from dotenv import load_dotenv
import os
import tempfile

load_dotenv()

# Service account JSON stored in environment variable
key_json_str = os.getenv("BIGQUERY_KEY_JSON")

if not key_json_str:
    raise ValueError("BIGQUERY_KEY_JSON environment variable is not set.")

# Use OS temp directory (works on Windows & Linux)
temp_dir = tempfile.gettempdir()
key_path = os.path.join(temp_dir, "bigquery-key.json")

# Write JSON to file
with open(key_path, "w") as f:
    f.write(key_json_str)

# Initialize BigQuery
client = bigquery.Client.from_service_account_json(key_path)

app = Flask(__name__)
CORS(app)

TABLE_NAME = os.getenv("BIGQUERY_TABLE", "master_viz")

def get_table():
    return f"`njc-ezpass.ezpass_data.{TABLE_NAME}`"


#Get all transactions with pagination
@app.route("/api/transactions")
def all_transactions():
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))  # Default 50 rows per page
        offset = (page - 1) * limit
        
        # Get search and filter parameters
        search = request.args.get('search', '').strip().replace("'", "''")  # Escape single quotes
        status_filter = request.args.get('status', '').strip().replace("'", "''")
        category_filter = request.args.get('category', '').strip().replace("'", "''")
        
        # Build WHERE clause
        where_conditions = []
        
        if search:
            # Handle NULL values properly in BigQuery using COALESCE
            # Search condition depends on table type
            if TABLE_NAME == "master_viz":
                where_conditions.append(f"(LOWER(COALESCE(CAST(transaction_id AS STRING), '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(tag_plate_number, '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(CAST(ml_predicted_category AS STRING), '')) LIKE LOWER('%{search}%'))")
            else:
                # gold_automation table
                where_conditions.append(f"(LOWER(COALESCE(CAST(transaction_id AS STRING), '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(tag_plate_number, '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(CAST(threat_severity AS STRING), '')) LIKE LOWER('%{search}%'))")
        
        if status_filter and status_filter != 'all':
            where_conditions.append(f"status = '{status_filter}'")
        
        if category_filter and category_filter != 'all':
            if TABLE_NAME == "master_viz":
                where_conditions.append(f"LOWER(ml_predicted_category) = LOWER('{category_filter}')")
            else:
                where_conditions.append(f"LOWER(threat_severity) = LOWER('{category_filter}')")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build query with pagination
        query = f"""
        SELECT * 
        FROM {get_table()}
        {where_clause}
        ORDER BY transaction_date DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = client.query(query).result()
        rows = [dict(row) for row in results]
        return jsonify({"data": rows, "page": page, "limit": limit})
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error fetching transactions: {str(e)}")
        print(f"Query: {query}")
        print(f"Traceback: {error_trace}")
        return jsonify({"data": [], "error": str(e), "query": query if 'query' in locals() else "N/A"}), 500

#Get total count of transactions (for pagination)
@app.route("/api/transactions/count")
def transactions_count():
    try:
        # Get filter parameters (same as all_transactions)
        search = request.args.get('search', '').strip().replace("'", "''")  # Escape single quotes
        status_filter = request.args.get('status', '').strip().replace("'", "''")
        category_filter = request.args.get('category', '').strip().replace("'", "''")
        
        # Build WHERE clause (same logic as all_transactions)
        where_conditions = []
        
        if search:
            # Handle NULL values properly in BigQuery using COALESCE
            # Search condition depends on table type
            if TABLE_NAME == "master_viz":
                where_conditions.append(f"(LOWER(COALESCE(CAST(transaction_id AS STRING), '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(tag_plate_number, '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(CAST(ml_predicted_category AS STRING), '')) LIKE LOWER('%{search}%'))")
            else:
                # gold_automation table
                where_conditions.append(f"(LOWER(COALESCE(CAST(transaction_id AS STRING), '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(tag_plate_number, '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(CAST(threat_severity AS STRING), '')) LIKE LOWER('%{search}%'))")
        
        if status_filter and status_filter != 'all':
            where_conditions.append(f"status = '{status_filter}'")
        
        if category_filter and category_filter != 'all':
            if TABLE_NAME == "master_viz":
                where_conditions.append(f"LOWER(ml_predicted_category) = LOWER('{category_filter}')")
            else:
                where_conditions.append(f"LOWER(threat_severity) = LOWER('{category_filter}')")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
        SELECT COUNT(*) AS total
        FROM {get_table()}
        {where_clause}
        """
        
        results = client.query(query).result()
        total = dict(next(results))["total"]
        return jsonify({"total": int(total)})
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error fetching transaction count: {str(e)}")
        print(f"Query: {query}")
        print(f"Traceback: {error_trace}")
        return jsonify({"total": 0, "error": str(e), "query": query if 'query' in locals() else "N/A"}), 500

#Get flagged or investigating transactions (Recent Alerts)
@app.route("/api/transactions/alerts")
def alerts():
    try:
        if TABLE_NAME == "master_viz":
            query = f"""
                SELECT * 
                FROM {get_table()} 
                WHERE is_anomaly = 1
                ORDER BY transaction_date DESC
                LIMIT 100
            """
        elif TABLE_NAME == "gold_automation":
            query = f"""
                SELECT * 
                FROM {get_table()} 
                WHERE flag_fraud = TRUE
                ORDER BY transaction_date DESC
                LIMIT 100
            """
        else:
            return jsonify({"error": "Unsupported table"}), 400

        results = client.query(query).result()
        rows = [dict(row) for row in results]
        return jsonify({"data": rows})

    except Exception as e:
        print(f"Error fetching alerts: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500


#Get recent flagged transactions for homepage card
@app.route("/api/transactions/recent-flagged")
def recent_flagged():
    try:
        if TABLE_NAME == "master_viz":
            query = f"""
                SELECT 
                    transaction_id,
                    transaction_date,
                    tag_plate_number,
                    agency,
                    amount,
                    status,
                    ml_predicted_category,
                    is_anomaly
                FROM {get_table()} 
                WHERE (status = 'Needs Review' OR is_anomaly = 1)
                ORDER BY transaction_date DESC
                LIMIT 3
            """
        elif TABLE_NAME == "gold_automation":
            query = f"""
                SELECT 
                    transaction_id,
                    transaction_date,
                    tag_plate_number,
                    agency,
                    amount,
                    status,
                    threat_severity,
                    flag_fraud
                FROM {get_table()} 
                WHERE (status = 'Needs Review' OR flag_fraud = TRUE)
                ORDER BY transaction_date DESC
                LIMIT 3
            """
        else:
            return jsonify({"error": "Unsupported table"}), 400

        results = client.query(query).result()
        rows = []

        for row in results:
            status = row.get('status')

            # Map category column depending on table
            category = row.get('ml_predicted_category') if TABLE_NAME == "master_viz" else row.get('threat_severity')
            if not category:
                category = 'Anomaly Detected'

            rows.append({
                'id': row.get('transaction_id'),
                'transaction_id': row.get('transaction_id'),
                'transaction_date': str(row.get('transaction_date')) if row.get('transaction_date') else None,
                'tag_plate_number': row.get('tag_plate_number'),
                'tagPlate': row.get('tag_plate_number'),
                'agency': row.get('agency'),
                'amount': float(row.get('amount')) if row.get('amount') is not None else 0.0,
                'status': status,
                'category': category,
                'is_anomaly': bool(row.get('is_anomaly') if TABLE_NAME == "master_viz" else row.get('flag_fraud'))
            })

        return jsonify({"data": rows})

    except Exception as e:
        print(f"Error fetching recent flagged transactions: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500


#Aggregated metrics for dashboard cards
@app.route("/api/metrics")
def metrics():
    try:
        if TABLE_NAME == "master_viz":
            query = f"""
                SELECT
                    COUNT(*) AS total_transactions,
                    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS total_flagged,
                    SUM(CASE WHEN is_anomaly = 1 THEN amount ELSE 0 END) AS total_amount,
                    SUM(CASE 
                            WHEN is_anomaly = 1 
                                 AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())
                            THEN amount 
                            ELSE 0 
                        END) AS potential_loss_ytd,
                    SUM(CASE WHEN is_anomaly = 1 
                             AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN 1 ELSE 0 END) AS total_alerts_ytd,
                    SUM(CASE 
                            WHEN ml_predicted_category IN ('Critical Risk', 'High Risk')
                                 AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())
                                 AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE())
                            THEN 1 
                            ELSE 0 
                        END) AS detected_frauds_current_month
                FROM {get_table()}
            """
        elif TABLE_NAME == "gold_automation":
            query = f"""
                SELECT
                    COUNT(*) AS total_transactions,
                    SUM(CASE WHEN flag_fraud = TRUE THEN 1 ELSE 0 END) AS total_flagged,
                    SUM(CASE WHEN flag_fraud = TRUE THEN amount ELSE 0 END) AS total_amount,
                    SUM(CASE 
                            WHEN flag_fraud = TRUE 
                                 AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())
                            THEN amount 
                            ELSE 0 
                        END) AS potential_loss_ytd,
                    SUM(CASE WHEN flag_fraud = TRUE 
                             AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN 1 ELSE 0 END) AS total_alerts_ytd,
                    SUM(CASE 
                            WHEN threat_severity IN ('Critical Risk', 'High Risk')
                                 AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())
                                 AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE())
                            THEN 1 
                            ELSE 0 
                        END) AS detected_frauds_current_month
                FROM {get_table()}
            """
        else:
            return jsonify({"error": "Unsupported table"}), 400

        results = client.query(query).result()
        metrics = dict(next(results))
        return jsonify({
            "total_transactions": int(metrics.get("total_transactions", 0)),
            "total_flagged": int(metrics.get("total_flagged", 0)),
            "total_amount": float(metrics.get("total_amount", 0)),
            "total_alerts_ytd": int(metrics.get("total_alerts_ytd", 0)),
            "detected_frauds_current_month": int(metrics.get("detected_frauds_current_month", 0)),
            "potential_loss_ytd": float(metrics.get("potential_loss_ytd", 0))
        })

    except Exception as e:
        print(f"Error fetching metrics: {str(e)}")
        return jsonify({
            "total_transactions": 0,
            "total_flagged": 0,
            "total_amount": 0,
            "total_alerts_ytd": 0,
            "detected_frauds_current_month": 0,
            "potential_loss_ytd": 0
        }), 500


#Fraud by Category for chart
@app.route("/api/charts/category")
def category_chart():

    # MASTER_VIZ VERSION (your original logic)
    if TABLE_NAME == "master_viz":
        query = f"""
            WITH unpivoted AS (
                SELECT
                    f.flag_label AS category
                FROM {get_table()},
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
            ORDER BY count DESC
        """

    # GOLD_AUTOMATION VERSION (based on your columns)
    elif TABLE_NAME == "gold_automation":
        # Filter by flag_fraud first, then show the contributing flags (excluding flag_fraud itself)
        query = f"""
            WITH unpivoted AS (
                SELECT
                    f.flag_label AS category
                FROM {get_table()},
                UNNEST([
                    STRUCT('Vehicle Type' AS flag_label, flag_vehicle_type AS flag_value),
                    STRUCT('Amount > 29' AS flag_label, flag_amount_gt_29 AS flag_value),
                    STRUCT('Out of State' AS flag_label, flag_is_out_of_state AS flag_value),
                    STRUCT('Weekend' AS flag_label, flag_is_weekend AS flag_value),
                    STRUCT('Holiday' AS flag_label, flag_is_holiday AS flag_value)
                ]) AS f
                WHERE flag_fraud = TRUE
                    AND f.flag_value IS TRUE
            )
            SELECT category, COUNT(*) AS count
            FROM unpivoted
            GROUP BY category
            ORDER BY count DESC
        """

    else:
        return jsonify({"error": "Unsupported table"}), 400

    # Run the query
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

    # MASTER_VIZ version (original behavior)
    if TABLE_NAME == "master_viz":
        query = f"""
            SELECT 
                ml_predicted_category AS severity,
                COUNT(*) AS count
            FROM {get_table()}
            WHERE ml_predicted_category IS NOT NULL
            GROUP BY ml_predicted_category
            ORDER BY count DESC
        """

    # GOLD_AUTOMATION version (uses threat_severity instead)
    elif TABLE_NAME == "gold_automation":
        query = f"""
            SELECT 
                threat_severity AS severity,
                COUNT(*) AS count
            FROM {get_table()}
            WHERE threat_severity IS NOT NULL
            GROUP BY threat_severity
            ORDER BY count DESC
        """

    else:
        return jsonify({"error": "Unsupported table"}), 400

    # Run query
    try:
        results = client.query(query).result()
        data = [{"severity": row["severity"], "count": row["count"]} for row in results]
        return jsonify({"data": data})
    except Exception as e:
        print("BACKEND ERROR:", e)
        return jsonify({"error": str(e), "data": []}), 500



#Monthly transaction analysis for bar chart
@app.route("/api/charts/monthly")
def monthly_chart():
    try:

        # MASTER_VIZ logic
        if TABLE_NAME == "master_viz":
            query = f"""
                SELECT 
                    FORMAT_DATE('%b %Y', DATE(transaction_date)) AS month,
                    EXTRACT(YEAR FROM DATE(transaction_date)) AS year,
                    EXTRACT(MONTH FROM DATE(transaction_date)) AS month_num,
                    COUNT(*) AS total_transactions,
                    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS fraud_alerts
                FROM {get_table()}
                WHERE transaction_date IS NOT NULL
                GROUP BY year, month_num, month
                ORDER BY year DESC, month_num DESC
                LIMIT 12
            """

        # GOLD_AUTOMATION logic
        elif TABLE_NAME == "gold_automation":
            query = f"""
                SELECT 
                    FORMAT_DATE('%b %Y', DATE(transaction_date)) AS month,
                    EXTRACT(YEAR FROM DATE(transaction_date)) AS year,
                    EXTRACT(MONTH FROM DATE(transaction_date)) AS month_num,
                    COUNT(*) AS total_transactions,
                    SUM(CASE WHEN flag_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_alerts
                FROM {get_table()}
                WHERE transaction_date IS NOT NULL
                GROUP BY year, month_num, month
                ORDER BY year DESC, month_num DESC
                LIMIT 12
            """

        else:
            return jsonify({"error": "Unsupported table"}), 400

        # Execute query
        results = client.query(query).result()
        
        data = [{
            "month": row["month"],
            "year": int(row["year"]),
            "month_num": int(row["month_num"]),
            "total_transactions": int(row["total_transactions"]),
            "fraud_alerts": int(row["fraud_alerts"] or 0)
        } for row in results]

        # Reverse to show oldest first if desired
        data.reverse()

        return jsonify({"data": data})

    except Exception as e:
        print(f"Error fetching monthly chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

#Scatter plot data for ml_anomaly_score vs amount
@app.route("/api/charts/scatter")
def scatter_chart():
    try:

        if TABLE_NAME == "master_viz":
            query = f"""
                SELECT 
                    amount,
                    ml_predicted_score AS ml_anomaly_score,
                    ml_predicted_category AS risk_level
                FROM {get_table()}
                WHERE DATE(transaction_date) >= '2024-01-01'
                    AND amount IS NOT NULL
                    AND ml_predicted_score IS NOT NULL
                    AND ml_predicted_category IS NOT NULL
            """

        elif TABLE_NAME == "gold_automation":
            query = f"""
                SELECT 
                    amount,
                    rule_based_score AS ml_anomaly_score,
                    threat_severity AS risk_level
                FROM {get_table()}
                WHERE DATE(transaction_date) >= '2024-01-01'
                    AND amount IS NOT NULL
                    AND rule_based_score IS NOT NULL
                    AND threat_severity IS NOT NULL
            """

        else:
            return jsonify({"error": "Unsupported table"}), 400

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

        if TABLE_NAME == "master_viz":
            query = f"""
                SELECT 
                    EXTRACT(HOUR FROM entry_time) AS hour,
                    COUNT(*) AS fraud_count
                FROM {get_table()}
                WHERE is_anomaly = 1
                GROUP BY hour
                ORDER BY hour
            """

        elif TABLE_NAME == "gold_automation":
            query = f"""
                SELECT 
                    EXTRACT(HOUR FROM entry_time) AS hour,
                    COUNT(*) AS fraud_count
                FROM {get_table()}
                WHERE flag_fraud = TRUE
                GROUP BY hour
                ORDER BY hour
            """

        else:
            return jsonify({"error": "Unsupported table"}), 400

        results = client.query(query).result()

        data = [{
            "hour": int(row["hour"]),
            "fraud_count": int(row["fraud_count"])
        } for row in results]

        return jsonify({"data": data})

    except Exception as e:
        print(f"Error fetching timeseries chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500
    
# Allowed status transitions
STATUS_TRANSITIONS = {
    "No Action Required": [],
    "Needs Review": ["Resolved - Not Fraud", "Investigating", "Resolved - Fraud"],
    "Investigating": ["Resolved - Not Fraud", "Resolved - Fraud"],
    "Resolved - Not Fraud": [],
    "Resolved - Fraud": []
}

@app.route("/api/transactions/update-status", methods=["POST"])
def update_status():
    try:
        data = request.get_json()
        transaction_id = data.get("transactionId")
        new_status = data.get("newStatus")

        if not transaction_id or not new_status:
            return jsonify({"error": "Missing transactionId or newStatus"}), 400

        # Escape single quotes in transaction_id to prevent SQL injection
        transaction_id_escaped = str(transaction_id).replace("'", "''")
        new_status_escaped = str(new_status).replace("'", "''")

        # Get current status
        query = f"""
            SELECT status
            FROM {get_table()}
            WHERE transaction_id = '{transaction_id_escaped}'
        """
        results = client.query(query).result()
        rows = list(results)
        
        if not rows:
            return jsonify({"error": "Transaction not found"}), 404

        current_status = rows[0].get("status")
        
        # Handle NULL status - allow setting status if current status is NULL
        if current_status is None:
            # Allow setting any status if current status is NULL
            pass
        else:
            # Validate transition
            allowed_transitions = STATUS_TRANSITIONS.get(current_status, [])
            if new_status not in allowed_transitions:
                return jsonify({
                    "error": "Invalid status transition",
                    "current_status": current_status,
                    "new_status": new_status,
                    "allowed": allowed_transitions
                }), 400

        # Update status - only update last_updated if the column exists
        if TABLE_NAME == "master_viz":
            update_query = f"""
                UPDATE {get_table()}
                SET status = '{new_status_escaped}', last_updated = CURRENT_TIMESTAMP()
                WHERE transaction_id = '{transaction_id_escaped}'
            """
        else:
            # gold_automation might not have last_updated column
            update_query = f"""
                UPDATE {get_table()}
                SET status = '{new_status_escaped}'
                WHERE transaction_id = '{transaction_id_escaped}'
            """

        client.query(update_query).result()

        return jsonify({"success": True})
    except Exception as e:
        print(f"Error updating transaction status: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/table-info")
def table_info():
    """Return information about the current table being used"""
    return jsonify({
        "table_name": TABLE_NAME,
        "is_master_viz": TABLE_NAME == "master_viz",
        "is_gold_automation": TABLE_NAME == "gold_automation"
    })



if __name__ == "__main__":
    app.run(debug=True, port=5001)