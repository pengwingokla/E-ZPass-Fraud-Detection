{{ config(
    materialized='table',
    partition_by={
        'field': 'transaction_date',
        'data_type': 'date'
    },
    cluster_by=['tag_plate_number', 'transaction_date'],
    tags=['gold']
) }}

SELECT 
    -- ===== CORE IDENTIFIERS =====
    transaction_id,
    transaction_date,
    posting_date,
    tag_plate_number,
    
    -- ===== LOCATION INFO =====
    agency,
    agency_name,
    state_name,
    entry_plaza,
    entry_plaza_name,
    entry_lane,
    exit_plaza,
    exit_plaza_name,
    exit_lane,
    entry_plaza_previous,
    exit_plaza_previous,
    route_instate,
    
    -- ===== TIME INFO =====
    entry_time,
    exit_time,
    entry_time_previous,
    exit_time_previous,
    entry_hour,
    exit_hour,
    -- entry_time_of_day,
    -- exit_time_of_day,
    travel_time_of_day,
    flag_rush_hour,
    -- transaction_dayofweek,
    -- transaction_dayofyear,
    -- transaction_month,
    -- transaction_day,
    flag_is_weekend,
    flag_is_holiday,
    
    -- ===== VEHICLE & FARE =====
    vehicle_type_code,
    vehicle_type_name,
    flag_vehicle_type,
    plan_rate,
    fare_type,
    
    -- ===== FINANCIAL =====
    amount,
    
    -- ===== VELOCITY & TRAVEL FEATURES (Silver) =====
    distance_miles,
    travel_time_minutes,
    speed_mph,
    min_required_travel_time_minutes,
    is_impossible_travel,
    is_rapid_succession,
    flag_overlapping_journey,
    overlapping_journey_duration_minutes,
    travel_time_category,
    
    -- ===== DRIVER ROLLING STATS (Silver) =====
    driver_amount_last_30txn_avg,
    driver_amount_last_30txn_std,
    driver_amount_last_30txn_min,
    driver_amount_last_30txn_max,
    driver_amount_last_30txn_count,
    driver_daily_txn_count,

    -- ===== GOLD LAYER: DRIVER FEATURES =====
    driver_amount_median,
    driver_amount_modified_z_score,
    amount_deviation_from_avg_pct,
    amount_deviation_from_median_pct,
    driver_today_spend,
    driver_avg_daily_spend_30d,
    
    -- ===== ROUTE STATS (Silver) =====
    route_name,
    route_amount_avg,
    route_amount_std,
    route_amount_min,
    route_amount_max,
    route_amount_med,
    route_transaction_count,

    -- ===== GOLD LAYER: ROUTE FEATURES =====
    route_amount_z_score,
    
    -- ===== GOLD LAYER: ANOMALY FLAGS =====
    flag_driver_amount_outlier,
    flag_route_amount_outlier,
    flag_amount_unusually_high,
    flag_driver_spend_spike,
    
    -- ===== GOLD LAYER: ANOMALY SCORE =====
    anomaly_score,
    
    -- ===== METADATA (last) =====
    last_updated,
    source_file

FROM {{ ref('_gold__score') }}