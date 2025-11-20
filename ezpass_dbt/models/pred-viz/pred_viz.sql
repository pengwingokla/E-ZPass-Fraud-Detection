{{ config(
    materialized='view',
    tags=['predictions', 'visualization']
) }}

SELECT 
    -- ===== CORE IDENTIFIERS =====
    transaction_id,
    
    -- ===== ML PREDICTIONS =====
    is_anomaly,
    ml_anomaly_score,
    prediction_timestamp,
    
    -- ===== RISK CATEGORIZATION =====
    anomaly_category,
    score_percentile_rank,
    p1 AS score_p1_threshold,   -- Critical Risk cutoff (bottom 1%)
    p5 AS score_p5_threshold,   -- High Risk cutoff (bottom 5%)
    p25 AS score_p25_threshold, -- Medium Risk cutoff (bottom 25%)
    
    -- ===== FINANCIAL =====
    amount,
    
    -- ===== VELOCITY & TRAVEL FEATURES =====
    distance_miles,
    travel_time_minutes,
    speed_mph,
    overlapping_journey_duration_minutes,
    
    -- ===== DRIVER ROLLING STATS =====
    driver_amount_last_30txn_avg,
    driver_amount_last_30txn_std,
    driver_amount_last_30txn_count,
    driver_daily_txn_count,
    
    -- ===== GOLD LAYER: DRIVER FEATURES =====
    driver_amount_modified_z_score,
    amount_deviation_from_avg_pct,
    amount_deviation_from_median_pct,
    driver_today_spend,
    driver_avg_daily_spend_30d,
    
    -- ===== GOLD LAYER: ROUTE FEATURES =====
    route_amount_z_score,
    
    -- ===== TIME & CONTEXT =====
    exit_hour,
    
    -- ===== VEHICLE =====
    vehicle_type_code,
    
    -- ===== ENCODED CATEGORICAL FEATURES =====
    route_name_freq_encoded,
    entry_plaza_freq_encoded,
    exit_plaza_freq_encoded,
    vehicle_type_freq_encoded,
    agency_freq_encoded,
    travel_time_of_day_freq_encoded

FROM {{ ref('_pred__quantile') }}

