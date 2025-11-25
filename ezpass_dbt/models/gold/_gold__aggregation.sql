{{ config(
    materialized='ephemeral',
    tags=['gold', 'base']
) }}

WITH source AS (
    SELECT * FROM {{ ref('silver') }}
),

aggregation AS (
    SELECT 

    transaction_id,
    tag_plate_number,
    driver_daily_txn_count,
    state_name,
    agency,
    agency_name,
    

    transaction_date,
    posting_date,


    

    entry_time,
    entry_plaza,
    entry_plaza_name,

    plaza_route,

    exit_time,
    exit_plaza,
    exit_plaza_name,

    --entry_time_previous,
    --exit_time_previous,
    

    --entry_plaza_previous,
    --exit_plaza_previous,
    --entry_lane,
    --exit_lane,
    --route_instate,

    distance_miles,
    travel_time_minutes,
    speed_mph,
    --min_required_travel_time_minutes,
    --is_impossible_travel,
    --is_rapid_succession,
    --is_overlapping_journey,
    --overlapping_journey_duration_minutes,
    
    --plan_rate,
    --fare_type,
    
    amount,

    --driver_amount_last_30txn_avg,
    --driver_amount_last_30txn_std,
    --driver_amount_last_30txn_min,
    --driver_amount_last_30txn_max,
    --driver_amount_last_30txn_count,
    --route_amount_avg,
    --route_amount_std,
    --route_amount_min,
    --route_amount_max,
    --route_amount_med,
    --route_transaction_count,
    
    ---- New features
    --transaction_dayofweek,
    --transaction_dayofyear,
    --transaction_month,
    --transaction_day,
    --entry_time_of_day,
    --exit_time_of_day,
    --travel_time_of_day,
    --entry_hour,
    --exit_hour,
    --travel_duration_category,
    vehicle_type_name,
    vehicle_type_code,

    -- Flags
    flag_vehicle_type,
    flag_is_weekend,
    flag_is_holiday,
    flag_outstate,
    flag_possible_cloning,

    -- is_missing_entry_plaza,
    -- is_missing_exit_plaza,
    -- is_missing_entry_time,
    -- is_missing_exit_time,

    -- Metadata (last)
    last_updated,
    source_file

    FROM source
)

SELECT * FROM aggregation