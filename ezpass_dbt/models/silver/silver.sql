{{ config(
    materialized='view',
    tags=['staging', 'silver']
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'bronze') }}
),

cleaned AS (
    SELECT
        -- Convert dates
        SAFE.PARSE_DATE('%Y-%m-%d', transaction_date) as transaction_date,
        SAFE.PARSE_DATE('%Y-%m-%d', posting_date) as posting_date,
        
        -- Convert timestamps
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', entry_time) as entry_time,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', exit_time) as exit_time,
        
        -- Clean text fields
        UPPER(TRIM(tag_plate_number)) as tag_plate_number,
        UPPER(TRIM(agency)) as agency,
        TRIM(description) as description,
        UPPER(TRIM(entry_plaza)) as entry_plaza,
        UPPER(TRIM(exit_plaza)) as exit_plaza,
        TRIM(entry_lane) as entry_lane,
        TRIM(exit_lane) as exit_lane,
        TRIM(vehicle_type_code) as vehicle_type_code,
        TRIM(plan_rate) as plan_rate,
        TRIM(fare_type) as fare_type,
        
        -- Convert numeric fields
        SAFE_CAST(amount AS FLOAT64) as amount,
        SAFE_CAST(prepaid AS FLOAT64) as prepaid,
        SAFE_CAST(balance AS FLOAT64) as balance,
        
        -- Metadata
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', loaded_at) as loaded_at,
        source_file
        
    FROM source
    WHERE transaction_date IS NOT NULL
)

SELECT * FROM cleaned