{{ config(
    materialized='ephemeral',
    tags=['silver', 'intermediate']
) }}

WITH enriched AS (
    SELECT * FROM {{ ref('_silver__enrichment') }}
),

new_features AS (
    SELECT
        -- Generate unique MD5 hash for transaction ID
        TO_HEX(MD5(CONCAT(
            COALESCE(CAST(transaction_date AS STRING), 'NULL'), '|',
            COALESCE(CAST(entry_time AS STRING), 'NULL'), '|',
            COALESCE(CAST(exit_time AS STRING), 'NULL'), '|',
            COALESCE(tag_plate_number, 'NULL'), '|',
            COALESCE(entry_plaza, 'NULL'), '|',
            COALESCE(exit_plaza, 'NULL'), '|',
            COALESCE(CAST(amount AS STRING), 'NULL')
        ))) as transaction_id,
        
        *,
        
        -- Feature engineering
        -- ============================================
        -- TIME-BASED FEATURES
        -- ============================================

        -- Extract datetime components
        -- EXTRACT(DAYOFWEEK FROM transaction_date) as transaction_dayofweek,  -- 1=Sun, 7=Sat
        -- EXTRACT(DAYOFYEAR FROM transaction_date) as transaction_dayofyear,
        -- FORMAT_DATE('%B', transaction_date) as transaction_month,
        -- FORMAT_DATE('%A', transaction_date) as transaction_day,

        -- Categorize days
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM transaction_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END as weekend_or_weekday,

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

        -- Flag for rush hour transactions
        CASE 
            WHEN (EXTRACT(HOUR FROM entry_time) BETWEEN 6 AND 8) 
                 OR (EXTRACT(HOUR FROM entry_time) BETWEEN 15 AND 18)
                 OR (EXTRACT(HOUR FROM exit_time) BETWEEN 6 AND 8) 
                 OR (EXTRACT(HOUR FROM exit_time) BETWEEN 15 AND 18)
            THEN TRUE
            ELSE FALSE
        END as flag_rush_hour,

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
        ) as travel_time_of_day,  -- e.g., "Morning Rush to Midday"

        -- Hour-by-hour patterns (0-23) 
        EXTRACT(HOUR FROM entry_time) as entry_hour,
        EXTRACT(HOUR FROM exit_time) as exit_hour,

        -- ============================================
        -- VEHICLE & USAGE FEATURES
        -- ============================================
        
        -- Vehicle class category
        
        CASE 
            WHEN vehicle_type_code IN ('1', '2', '2L', '2H') THEN 'Passenger Vehicle'
            WHEN vehicle_type_code IN ('3', '3L', '3H', 'B2', 'B3') THEN 'Light Commercial'
            WHEN vehicle_type_code IN ('4', '5', '6', '7', '8', '9', '4L', '4H', '5H', '6H', '7H') THEN 'Heavy Commercial'
            ELSE 'Unknown'
        END as vehicle_type_name,
        
        -- Daily Number of Transactions by tag_plate_number
        COUNT(*) OVER (PARTITION BY tag_plate_number, transaction_date) AS driver_daily_txn_count

    FROM enriched
)

SELECT * EXCEPT(entry_time_of_day, exit_time_of_day) FROM new_features