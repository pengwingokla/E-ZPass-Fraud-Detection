{{ config(
    materialized='table',
    partition_by={
        'field': 'transaction_date',
        'data_type': 'date'
    },
    cluster_by=['tag_plate_number', 'transaction_date'],
    tags=['gold', 'training']
) }}

SELECT 
    -- ===== CORE IDENTIFIERS =====
    transaction_id,
    transaction_date,
    tag_plate_number,
    
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
    -- entry_hour,
    exit_hour,
    -- travel_time_of_day,  -- Using encoded version instead
    -- flag_rush_hour,  -- Excluded from training
    -- flag_is_weekend,  -- Excluded from training
    
    -- ===== FINANCIAL =====
    amount,
    
    -- ===== VEHICLE =====
    -- flag_vehicle_type,
    vehicle_type_code,
    
    -- ===== CATEGORICAL FEATURES (raw) =====
    -- route_name,
    -- entry_plaza,
    -- exit_plaza,
    -- vehicle_type_code,
    -- agency,
    
    -- ===== ENCODED CATEGORICAL FEATURES =====
    route_name_freq_encoded,
    entry_plaza_freq_encoded,
    exit_plaza_freq_encoded,
    vehicle_type_freq_encoded,
    agency_freq_encoded,
    travel_time_of_day_freq_encoded

FROM {{ ref('_gold__encode') }}

