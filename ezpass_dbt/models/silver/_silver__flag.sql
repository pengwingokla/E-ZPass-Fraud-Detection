{{ config(
    materialized='ephemeral',
    tags=['silver', 'intermediate']
) }}

WITH new_features AS (
    SELECT * FROM {{ ref('_silver__enrichment') }}
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

    FROM new_features
)

SELECT * FROM flagged