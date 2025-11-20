{{ config(
    materialized='table',
    partition_by={
        'field': 'transaction_date',
        'data_type': 'date'
    },
    cluster_by=['tag_plate_number', 'ml_predicted_category', 'transaction_date'],
    tags=['master', 'visualization']
) }}

WITH predictions AS (
    SELECT * FROM {{ ref('pred_viz') }}
),

gold_data AS (
    SELECT * FROM {{ ref('gold') }}
)

SELECT 
    -- ===== CORE IDENTIFIERS =====

    
    -- Status column for workflow management
    CASE 
        WHEN LOWER(p.anomaly_category) IN ('high risk', 'critical risk') THEN 'Needs Review'
        WHEN p.anomaly_category IS NULL THEN NULL
        ELSE 'No Action Required'
    END AS status,
    
    g.transaction_id,
    g.transaction_date,
    g.posting_date,
    g.tag_plate_number,
    
    -- ===== ML PREDICTIONS & RISK =====
    p.is_anomaly,
    g.anomaly_score AS rule_based_score,
    p.ml_anomaly_score AS ml_predicted_score,
    p.anomaly_category AS ml_predicted_category,
    
    -- p.score_percentile_rank,
    -- p.score_p1_threshold,
    -- p.score_p5_threshold,
    -- p.score_p25_threshold,
    
    -- ===== LOCATION INFO =====
    g.agency,
    g.route_name,
    g.route_instate,
    -- g.agency_name,
    g.state_name,
    g.entry_plaza,
    -- g.entry_plaza_name,
    g.entry_lane,
    g.exit_plaza,
    -- g.exit_plaza_name,
    g.exit_lane,
    
    -- ===== TIME INFO =====
    g.entry_time,
    g.exit_time,
    -- g.entry_hour,
    -- g.exit_hour,
    -- g.travel_time_of_day,
    
    -- ===== VEHICLE & FARE =====
    -- g.vehicle_type_code,
    g.vehicle_type_name,
    -- g.flag_vehicle_type,
    g.plan_rate,
    g.fare_type,
    
    -- ===== FINANCIAL =====
    g.amount,
    
    -- ===== VELOCITY & TRAVEL FEATURES =====
    g.distance_miles,
    g.travel_time_minutes,
    g.speed_mph,
    -- g.min_required_travel_time_minutes,
    g.is_impossible_travel,
    g.is_rapid_succession,
    -- g.overlapping_journey_duration_minutes,
    g.travel_time_category,
    
    -- ===== DRIVER ROLLING STATS =====
    -- g.driver_amount_last_30txn_avg,
    -- g.driver_amount_last_30txn_std,
    -- g.driver_amount_last_30txn_min,
    -- g.driver_amount_last_30txn_max,
    -- g.driver_amount_last_30txn_count,
    -- g.driver_daily_txn_count,
    
    -- ===== DRIVER FEATURES =====
    -- g.driver_amount_median,
    -- g.driver_amount_modified_z_score,
    -- g.amount_deviation_from_avg_pct,
    -- g.amount_deviation_from_median_pct,
    -- g.driver_today_spend,
    -- g.driver_avg_daily_spend_30d,
    
    -- ===== ROUTE STATS =====
    -- g.route_amount_avg,
    -- g.route_amount_std,
    -- g.route_amount_min,
    -- g.route_amount_max,
    -- g.route_amount_med,
    -- g.route_transaction_count,
    -- g.route_amount_z_score,
    
    -- ===== RULE-BASED ANOMALY FLAGS =====
    g.flag_rush_hour,
    g.flag_is_weekend,
    g.flag_is_holiday,
    g.flag_overlapping_journey,
    g.flag_driver_amount_outlier,
    g.flag_route_amount_outlier,
    g.flag_amount_unusually_high,
    g.flag_driver_spend_spike,
    
    -- ===== METADATA =====
    p.prediction_timestamp,
    g.last_updated,
    -- g.source_file

FROM gold_data g
LEFT JOIN predictions p
    ON g.transaction_id = p.transaction_id

