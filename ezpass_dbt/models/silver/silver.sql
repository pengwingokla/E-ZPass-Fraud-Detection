{{ config(
    materialized='table',
    partition_by={
        'field': 'transaction_date',
        'data_type': 'date'
    },
    cluster_by=['tag_plate_number', 'transaction_date'],
    tags=['silver'],
) }}

WITH silver_final AS (
    SELECT * FROM {{ ref('_silver__flag') }}
)

SELECT 
    -- Transaction ID
    transaction_id,
    
    -- Dates
    transaction_date,
    posting_date,

    -- IDs
    tag_plate_number,
    agency,
    agency_name,
    
    -- Timestamps
    entry_time,
    exit_time,
    entry_time_previous,
    exit_time_previous,
    
    -- Removed description as it only has 'TOLL' as value
    -- description,
    
    -- Plaza info
    entry_plaza,
    exit_plaza,
    entry_plaza_previous,
    exit_plaza_previous,
    plaza_route,
    entry_plaza_name,
    exit_plaza_name,
    entry_lane,
    exit_lane,
    route_instate,

    -- Velocity and Travel Features
    distance_miles,
    travel_time_minutes,
    speed_mph,
    min_required_travel_time_minutes,
    is_impossible_travel,
    is_rapid_succession,
    is_overlapping_journey,
    overlapping_journey_duration_minutes,
    flag_possible_cloning,
    
    -- Vehicle & fare
    vehicle_type_code,
    plan_rate,
    fare_type,
    
    -- Financial
    amount,
    -- Removed prepaid as it only has 'Y' as value
    -- prepaid,
    -- Removed from cleaning layer
    -- balance, 

    -- Price features
    driver_amount_last_30txn_avg,
    driver_amount_last_30txn_std,
    driver_amount_last_30txn_min,
    driver_amount_last_30txn_max,
    driver_amount_last_30txn_count,
    route_amount_avg,
    route_amount_std,
    route_amount_min,
    route_amount_max,
    route_amount_med,
    route_transaction_count,
    
    -- New features
    tag_daily_txn_count,
    state_name,
    transaction_dayofweek,
    transaction_dayofyear,
    transaction_month,
    transaction_day,
    entry_time_of_day,
    exit_time_of_day,
    travel_time_of_day,
    entry_hour,
    exit_hour,
    travel_duration_category,
    vehicle_type_name,
    vehicle_type_class,

    -- Flags
    is_weekend,
    is_holiday,
    -- is_missing_entry_plaza,
    -- is_missing_exit_plaza,
    -- is_missing_entry_time,
    -- is_missing_exit_time,

    -- Metadata (last)
    loaded_at as last_updated,
    source_file

FROM silver_final