import os

from flask import Flask, jsonify, request, send_from_directory
from werkzeug.utils import secure_filename
from flask_cors import CORS

from config import client, get_metrics_table, get_master_viz_table, get_stats_month_table

# Initialize Flask app
# If static folder exists (from Docker build), serve frontend from there
static_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.exists(static_folder):
    app = Flask(__name__, static_folder=static_folder, static_url_path="")
else:
    app = Flask(__name__)

CORS(app)


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
        
        # Build WHERE clause (Transactions tab: master_viz columns only)
        where_conditions = []
        if search:
            where_conditions.append(f"(LOWER(COALESCE(CAST(transaction_id AS STRING), '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(tag_plate_number, '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(CAST(ml_predicted_category AS STRING), '')) LIKE LOWER('%{search}%'))")
        if status_filter and status_filter != 'all':
            where_conditions.append(f"status = '{status_filter}'")
        if category_filter and category_filter != 'all':
            where_conditions.append(f"LOWER(ml_predicted_category) = LOWER('{category_filter}')")
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Transactions tab: master_viz table only
        query = f"""
        SELECT * 
        FROM {get_master_viz_table()}
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
        
        # Build WHERE clause (same as all_transactions); Transactions tab: master_viz only
        where_conditions = []
        if search:
            where_conditions.append(f"(LOWER(COALESCE(CAST(transaction_id AS STRING), '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(tag_plate_number, '')) LIKE LOWER('%{search}%') OR LOWER(COALESCE(CAST(ml_predicted_category AS STRING), '')) LIKE LOWER('%{search}%'))")
        if status_filter and status_filter != 'all':
            where_conditions.append(f"status = '{status_filter}'")
        if category_filter and category_filter != 'all':
            where_conditions.append(f"LOWER(ml_predicted_category) = LOWER('{category_filter}')")
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Transactions tab: always use master_viz table
        query = f"""
        SELECT COUNT(*) AS total
        FROM {get_master_viz_table()}
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

# Get flagged or investigating transactions (Recent Alerts). Transactions tab: master_viz only.
@app.route("/api/transactions/alerts")
def alerts():
    try:
        query = f"""
            SELECT * 
            FROM {get_master_viz_table()} 
            WHERE is_anomaly = 1
            ORDER BY transaction_date DESC
            LIMIT 100
        """
        results = client.query(query).result()
        rows = [dict(row) for row in results]
        return jsonify({"data": rows})

    except Exception as e:
        print(f"Error fetching alerts: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500


# Recent flagged transactions for homepage card. Transactions tab: master_viz only.
@app.route("/api/transactions/recent-flagged")
def recent_flagged():
    try:
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
            FROM {get_master_viz_table()} 
            WHERE (status = 'Needs Review' OR is_anomaly = 1)
            ORDER BY transaction_date DESC
            LIMIT 3
        """
        results = client.query(query).result()
        rows = []

        for row in results:
            status = row.get('status')
            category = row.get('ml_predicted_category') or 'Anomaly Detected'
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
                'is_anomaly': bool(row.get('is_anomaly'))
            })

        return jsonify({"data": rows})

    except Exception as e:
        print(f"Error fetching recent flagged transactions: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500


# Dashboard stats: from _stats only.
@app.route("/api/metrics")
def metrics():
    try:
        query = f"""
            SELECT
                as_of_date,
                total_transactions,
                total_amount_all_time,
                loss_all_time,
                total_alert_all_time
            FROM {get_metrics_table()}
            ORDER BY as_of_date DESC
            LIMIT 1
        """
        results = client.query(query).result()
        row = next(results, None)
        if row is None:
            return jsonify({
                "total_transactions": 0,
                "total_amount_all_time": 0,
                "total_alert_all_time": 0,
                "loss_all_time": 0,
            })
        m = dict(row)
        return jsonify({
            "total_transactions": int(m.get("total_transactions", 0)),
            "total_amount_all_time": float(m.get("total_amount_all_time", 0)),
            "total_alert_all_time": int(m.get("total_alert_all_time", 0)),
            "loss_all_time": float(m.get("loss_all_time") or 0),
        })
    except Exception as e:
        print(f"Error fetching metrics: {str(e)}")
        return jsonify({
            "total_transactions": 0,
            "total_amount_all_time": 0,
            "total_alert_all_time": 0,
            "loss_all_time": 0,
        }), 500


# Latest month stats from _stats_month (for Detected Frauds card)
@app.route("/api/metrics/latest-month")
def metrics_latest_month():
    try:
        query = f"""
            SELECT month, total_alerts
            FROM {get_stats_month_table()}
            ORDER BY month DESC
            LIMIT 1
        """
        results = client.query(query).result()
        row = next(results, None)
        if row is None:
            return jsonify({"month": None, "total_alerts": 0})
        return jsonify({
            "month": str(row["month"]) if row["month"] else None,
            "total_alerts": int(row["total_alerts"] or 0),
        })
    except Exception as e:
        print(f"Error fetching latest month metrics: {str(e)}")
        return jsonify({"month": None, "total_alerts": 0, "error": str(e)}), 500


# Fraud by Category for chart (master_viz only)
@app.route("/api/charts/category")
def category_chart():
    query = f"""
        WITH unpivoted AS (
            SELECT
                f.flag_label AS category
            FROM {get_master_viz_table()},
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
    # Run the query
    try:
        results = client.query(query).result()
        data = [{"category": row["category"], "count": row["count"]} for row in results]
        return jsonify({"data": data})
    except Exception as e:
        print("BACKEND ERROR:", e)
        return jsonify({"error": str(e), "data": []}), 500



# Threat Severity for chart (master_viz only)
@app.route("/api/charts/severity")
def severity_chart():
    query = f"""
        SELECT 
            ml_predicted_category AS severity,
            COUNT(*) AS count
        FROM {get_master_viz_table()}
        WHERE ml_predicted_category IS NOT NULL
        GROUP BY ml_predicted_category
        ORDER BY count DESC
    """
    # Run query
    try:
        results = client.query(query).result()
        data = [{"severity": row["severity"], "count": row["count"]} for row in results]
        return jsonify({"data": data})
    except Exception as e:
        print("BACKEND ERROR:", e)
        return jsonify({"error": str(e), "data": []}), 500



# Monthly transaction analysis for bar chart (_stats_month: month as YYYY-MM)
@app.route("/api/charts/monthly")
def monthly_chart():
    try:
        query = f"""
            SELECT
                month,
                total_transactions,
                total_alerts,
                total_loss
            FROM {get_stats_month_table()}
            ORDER BY month DESC
            LIMIT 12
        """
        results = client.query(query).result()

        data = [{
            "month": str(row["month"]),
            "total_transactions": int(row["total_transactions"] or 0),
            "fraud_alerts": int(row["total_alerts"] or 0),
            "total_loss": float(row["total_loss"] or 0),
        } for row in results]

        # Oldest month first for chart order
        data.reverse()

        return jsonify({"data": data})

    except Exception as e:
        print(f"Error fetching monthly chart data: {str(e)}")
        return jsonify({"data": [], "error": str(e)}), 500

# Scatter plot data for ml_anomaly_score vs amount (master_viz only)
@app.route("/api/charts/scatter")
def scatter_chart():
    try:
        query = f"""
            SELECT 
                amount,
                ml_predicted_score AS ml_anomaly_score,
                ml_predicted_category AS risk_level
            FROM {get_master_viz_table()}
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


# Time series data for anomaly counts by hour (master_viz only)
@app.route("/api/charts/timeseries")
def timeseries_chart():
    try:
        query = f"""
            SELECT 
                EXTRACT(HOUR FROM entry_time) AS hour,
                COUNT(*) AS fraud_count
            FROM {get_master_viz_table()}
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

        # Transactions tab: master_viz only
        query = f"""
            SELECT status
            FROM {get_master_viz_table()}
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

        update_query = f"""
            UPDATE {get_master_viz_table()}
            SET status = '{new_status_escaped}', last_updated = CURRENT_TIMESTAMP()
            WHERE transaction_id = '{transaction_id_escaped}'
        """
        client.query(update_query).result()

        return jsonify({"success": True})
    except Exception as e:
        print(f"Error updating transaction status: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/table-info")
def table_info():
    """Return information about the table being used (master_viz only)."""
    return jsonify({
        "table_name": "master_viz",
        "is_master_viz": True,
    })


@app.route("/api/upload-csv", methods=["POST"])
def upload_csv():
    """
    Accept a single CSV file upload and save it under data/raw/.
    """
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file part in request"}), 400

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"error": "No selected file"}), 400

        filename = secure_filename(file.filename)
        if not filename.lower().endswith(".csv"):
            return jsonify({"error": "Only .csv files are allowed"}), 400

        # Resolve data/raw directory relative to project root
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(backend_dir, os.pardir))
        raw_dir = os.path.join(project_root, "data", "raw")
        os.makedirs(raw_dir, exist_ok=True)

        save_path = os.path.join(raw_dir, filename)
        file.save(save_path)

        return jsonify({"success": True, "filename": filename}), 200
    except Exception as e:
        print(f"Error uploading CSV: {e}")
        return jsonify({"error": str(e)}), 500


# Serve React app for all non-API routes (only if static folder exists)
if os.path.exists(static_folder):
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve(path):
        """Serve React app for all non-API routes"""
        # Don't serve API routes as static files
        if path.startswith('api/'):
            return jsonify({"error": "Not found"}), 404
        
        # Serve index.html for all routes (React Router)
        if path != "" and os.path.exists(os.path.join(static_folder, path)):
            return send_from_directory(static_folder, path)
        else:
            return send_from_directory(static_folder, 'index.html')


if __name__ == "__main__":
    app.run(debug=True, port=5001)
