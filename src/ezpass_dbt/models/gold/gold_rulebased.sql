{{ config(
    materialized='table',
    partition_by={
        'field': 'transaction_date',
        'data_type': 'date'
    },
    cluster_by=['tag_plate_number', 'transaction_date'],
    tags=['gold', 'rulebased']
) }}

SELECT 
    -- ===== CORE IDENTIFIERS =====
    transaction_id,
    transaction_date,
    tag_plate_number,
    
    -- ===== LOCATION INFO =====
    agency_name,
    state_name,
    -- entry_plaza_name,
    -- exit_plaza_name,
    route_name,
    route_instate,
    
    -- ===== TIME INFO =====
    -- entry_time,
    -- exit_time,
    -- entry_hour,
    -- exit_hour,
    -- travel_time_of_day,

    -- ===== VEHICLE & FARE =====
    vehicle_type_code,
    -- vehicle_type_name,
    -- is_commercial_vehicle,
    -- fare_type,
    
    -- ===== FINANCIAL =====
    amount,
    
    -- ===== TRAVEL METRICS =====
    -- distance_miles,
    -- travel_time_minutes,
    -- speed_mph,
    travel_time_category,
    
    -- ===== DRIVER BEHAVIOR =====
    -- driver_amount_last_30txn_avg,
    -- driver_amount_median,
    driver_amount_modified_z_score,
    -- amount_deviation_from_avg_pct,
    -- amount_deviation_from_median_pct,
    -- driver_today_spend,
    -- driver_avg_daily_spend_30d,
    -- driver_daily_txn_count,
    
    -- ===== ROUTE METRICS =====
    -- route_amount_avg,
    -- route_amount_med,
    route_amount_z_score,
    
    -- ===== ANOMALY FLAGS =====
    flag_is_weekend,
    flag_is_holiday,
    flag_rush_hour,
    is_impossible_travel,
    is_rapid_succession,
    flag_overlapping_journey,
    flag_driver_amount_outlier,
    flag_route_amount_outlier,
    flag_amount_unusually_high,
    flag_driver_spend_spike,
    
    -- ===== ANOMALY SCORE =====
    anomaly_score,
    
    -- ===== METADATA =====
    last_updated

FROM {{ ref('gold') }}


