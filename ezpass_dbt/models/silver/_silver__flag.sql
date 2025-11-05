{{ config(
    materialized='ephemeral',
    tags=['silver', 'intermediate']
) }}

WITH new_features AS (
    SELECT * FROM {{ ref('_silver__feateng') }}
),

flagged AS (
    SELECT
        *,
        -- ============================================
        -- DATA QUALITY FLAGS
        -- ============================================
        
        -- Missing data flags
        CASE WHEN entry_plaza IS NULL THEN TRUE ELSE FALSE END as is_missing_entry,
        CASE WHEN exit_plaza IS NULL THEN TRUE ELSE FALSE END as is_missing_exit,
        CASE WHEN entry_time IS NULL THEN TRUE ELSE FALSE END as is_missing_entry_time,
        CASE WHEN exit_time IS NULL THEN TRUE ELSE FALSE END as is_missing_exit_time,

        -- Usage Outside Business flags
        -- Checks if a given date is a weekend (Saturday or Sunday)
        CASE is_weekend
            WHEN 'Weekend' THEN TRUE
            WHEN 'Weekday' THEN FALSE
            ELSE NULL
        END as flag_is_weekend,

        -- Checks if a person has traveled from NJ to a neighboring state: NY, PA, DE and vice versa. Also flags people using tolls outside the state of NJ
        CASE state_name
            WHEN 'NJ' THEN FALSE
            ELSE TRUE
        END as flag_is_out_of_state,

        -- Checks if the provided vehicle_class_code is larger than allowed
        CASE vehicle_class_category
            WHEN 'Light Commercial' THEN TRUE
            WHEN 'Heavy Commercial' THEN TRUE
            ELSE FALSE
        END as flag_is_vehicle_type_gt2

    FROM new_features
)

SELECT * FROM flagged