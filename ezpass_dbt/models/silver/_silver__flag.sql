{{ config(
    materialized='ephemeral',
    tags=['silver', 'intermediate']
) }}

WITH base_features AS (
    SELECT * FROM {{ ref('_silver__feateng_price') }}
),

holidays AS (
    SELECT * FROM {{ source('raw', 'holidays') }}
),

flagged AS (
    SELECT
        *,
        -- ============================================
        -- DATA QUALITY FLAGS
        -- ============================================
        
        -- Missing data flags
        -- CASE WHEN entry_plaza IS NULL THEN TRUE ELSE FALSE END as is_missing_entry_plaza,
        -- CASE WHEN exit_plaza IS NULL THEN TRUE ELSE FALSE END as is_missing_exit_plaza,
        -- CASE WHEN entry_time IS NULL THEN TRUE ELSE FALSE END as is_missing_entry_time,
        -- CASE WHEN exit_time IS NULL THEN TRUE ELSE FALSE END as is_missing_exit_time,

        -- Usage Outside Business flags
        -- Checks if a given date is a weekend (Saturday or Sunday)
        CASE weekend_or_weekday
            WHEN 'Weekend' THEN TRUE
            WHEN 'Weekday' THEN FALSE
            ELSE NULL
        END as is_weekend,

        -- Checks if the provided transaction_date is considered a NJ Courts Holiday
        CASE 
            WHEN EXISTS (
                SELECT 1
                FROM holidays
                WHERE holiday_date = transaction_date
            ) THEN TRUE
            ELSE FALSE
        END AS is_holiday,

        -- Checks if the provided vehicle_class_code is larger than allowed
        CASE vehicle_type_name
            WHEN 'Light Commercial' THEN TRUE
            WHEN 'Heavy Commercial' THEN TRUE
            ELSE FALSE
        END as vehicle_type_class

    FROM base_features
)

SELECT * FROM flagged
