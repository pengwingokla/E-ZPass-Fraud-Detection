{{ config(
    materialized='view',
    tags=['silver']
) }}

SELECT 
    -- Dates
    transaction_date,
    posting_date,
    
    -- Timestamps
    entry_time,
    exit_time,
    
    -- IDs
    tag_plate_number,
    agency,
    agency_name,
    description,
    
    -- Entry info
    entry_plaza,
    entry_plaza_name,
    entry_lane,
    
    -- Exit info
    exit_plaza,
    exit_plaza_name,
    exit_lane,
    
    -- Vehicle & fare
    vehicle_type_code,
    plan_rate,
    fare_type,
    
    -- Financial
    amount,
    prepaid,
    balance,
    
    -- New features
    transaction_dayofweek,
    transaction_dayofyear,
    transaction_month,
    transaction_day,
    is_weekend,
    entry_time_of_day,
    exit_time_of_day,
    journey_time_of_day,
    entry_hour,
    exit_hour,
    travel_duration_category,
    vehicle_class_category,

    -- Metadata (last)
    loaded_at as last_updated,
    source_file
FROM {{ ref('_silver__feateng') }}