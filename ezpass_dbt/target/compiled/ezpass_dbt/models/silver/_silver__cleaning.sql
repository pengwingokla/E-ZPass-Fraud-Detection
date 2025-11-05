

WITH source AS (
    SELECT * FROM `njc-ezpass`.`ezpass_data`.`bronze`
),

cleaned AS (
    SELECT
        -- Convert dates (STRING YYYY-MM-DD → DATE)
        SAFE.PARSE_DATE('%Y-%m-%d', transaction_date) as transaction_date,
        SAFE.PARSE_DATE('%Y-%m-%d', posting_date) as posting_date,
        
        -- Convert timestamps (STRING YYYY-MM-DD HH:MM:SS → TIMESTAMP)
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', entry_time) as entry_time,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', exit_time) as exit_time,
        
        -- Clean and standardize text fields (replace '-' with NULL)
        NULLIF(UPPER(TRIM(tag_plate_number)), '-') as tag_plate_number,
        NULLIF(UPPER(TRIM(agency)), '-') as agency,
        NULLIF(TRIM(description), '-') as description,
        NULLIF(UPPER(TRIM(entry_plaza)), '-') as entry_plaza,
        NULLIF(TRIM(entry_lane), '-') as entry_lane,
        NULLIF(UPPER(TRIM(exit_plaza)), '-') as exit_plaza,
        NULLIF(TRIM(exit_lane), '-') as exit_lane,
        NULLIF(TRIM(vehicle_type_code), '-') as vehicle_type_code,
        NULLIF(TRIM(plan_rate), '-') as plan_rate,
        NULLIF(TRIM(prepaid), '-') as prepaid,
        NULLIF(TRIM(fare_type), '-') as fare_type,
        
        -- Convert numeric fields (STRING → FLOAT64)
        SAFE_CAST(amount AS FLOAT64) as amount,
        SAFE_CAST(balance AS FLOAT64) as balance,
        
        -- Keep metadata
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', loaded_at) as loaded_at,
        source_file
        
    FROM source
    WHERE transaction_date IS NOT NULL  -- Filter out bad records
)

SELECT * FROM cleaned