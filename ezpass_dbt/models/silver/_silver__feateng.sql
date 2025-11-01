{{ config(
    materialized='ephemeral',
    tags=['silver', 'intermediate']
) }}

WITH enriched AS (
    SELECT * FROM {{ ref('_silver__enrichment') }}
),

new_features AS (
    SELECT
        *,
        -- Feature engineering
        -- ============================================
        -- TIME-BASED FEATURES
        -- ============================================

        -- Extract datetime components
        EXTRACT(DAYOFWEEK FROM transaction_date) as transaction_dayofweek,  -- 1=Sun, 7=Sat
        EXTRACT(DAYOFYEAR FROM transaction_date) as transaction_dayofyear,
        FORMAT_DATE('%B', transaction_date) as transaction_month,
        FORMAT_DATE('%A', transaction_date) as transaction_day,

        -- Categorize days
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM transaction_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END as is_weekend,

        -- TIME OF DAY FEATURES
        -- Entry time of day
        CASE 
            WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 0 AND 5 THEN 'Night'
            WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 6 AND 8 THEN 'Morning Rush'
            WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 9 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 12 AND 14 THEN 'Midday'
            WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 15 AND 18 THEN 'Evening Rush'
            WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 19 AND 21 THEN 'Evening'
            ELSE 'Late Night'
        END as entry_time_of_day,

        -- Exit time of day
        CASE 
            WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 0 AND 5 THEN 'Night'
            WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 6 AND 8 THEN 'Morning Rush'
            WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 9 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 12 AND 14 THEN 'Midday'
            WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 15 AND 18 THEN 'Evening Rush'
            WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 19 AND 21 THEN 'Evening'
            ELSE 'Late Night'
        END as exit_time_of_day,

        -- Combined journey category
        CONCAT(
            CASE 
                WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 0 AND 5 THEN 'Night'
                WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 6 AND 8 THEN 'Morning Rush'
                WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 9 AND 11 THEN 'Morning'
                WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 12 AND 14 THEN 'Midday'
                WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 15 AND 18 THEN 'Evening Rush'
                WHEN EXTRACT(HOUR FROM entry_time) BETWEEN 19 AND 21 THEN 'Evening'
                ELSE 'Late Night'
            END,
            ' to ',
            CASE 
                WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 0 AND 5 THEN 'Night'
                WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 6 AND 8 THEN 'Morning Rush'
                WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 9 AND 11 THEN 'Morning'
                WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 12 AND 14 THEN 'Midday'
                WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 15 AND 18 THEN 'Evening Rush'
                WHEN EXTRACT(HOUR FROM exit_time) BETWEEN 19 AND 21 THEN 'Evening'
                ELSE 'Late Night'
            END
        ) as journey_time_of_day,  -- e.g., "Morning Rush to Midday"

        -- Hour-by-hour patterns (0-23) 
        EXTRACT(HOUR FROM entry_time) as entry_hour,
        EXTRACT(HOUR FROM exit_time) as exit_hour,
        
        -- Duration categories
        CASE 
            WHEN TIMESTAMP_DIFF(exit_time, entry_time, MINUTE) IS NULL THEN ''
            WHEN TIMESTAMP_DIFF(exit_time, entry_time, MINUTE) < 5 THEN 'Very Short (<5 min)'
            WHEN TIMESTAMP_DIFF(exit_time, entry_time, MINUTE) < 15 THEN 'Short (5-15 min)'
            WHEN TIMESTAMP_DIFF(exit_time, entry_time, MINUTE) < 30 THEN 'Medium (15-30 min)'
            WHEN TIMESTAMP_DIFF(exit_time, entry_time, MINUTE) < 60 THEN 'Long (30-60 min)'
            ELSE 'Very Long (60+ min)'
        END as travel_duration_category,

        -- ============================================
        -- VEHICLE & USAGE FEATURES
        -- ============================================
        
        -- Vehicle class category
        CASE 
            WHEN vehicle_type_code IN ('1', '2') THEN 'Passenger Vehicle'
            WHEN vehicle_type_code IN ('3', '4', '5') THEN 'Light Commercial'
            WHEN vehicle_type_code IN ('6', '7', '8', '9') THEN 'Heavy Commercial'
            ELSE 'Unknown'
        END as vehicle_class_category,
        
    FROM enriched
)

SELECT * FROM new_features