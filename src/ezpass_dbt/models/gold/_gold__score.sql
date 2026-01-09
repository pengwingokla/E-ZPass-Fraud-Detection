{{ config(
    materialized='ephemeral',
    tags=['gold', 'base']
) }}

WITH flags AS (
    SELECT * FROM {{ ref('_gold__flags') }}
),

anomaly_scoring AS (
    SELECT
        *,
        
        -- Weighted anomaly score: sum of flag weights
        (CASE WHEN flag_driver_amount_outlier THEN 80 ELSE 0 END) +
        (CASE WHEN flag_route_amount_outlier THEN 10 ELSE 0 END) +
        (CASE WHEN flag_amount_unusually_high THEN 5 ELSE 0 END) +
        (CASE WHEN flag_driver_spend_spike THEN 5 ELSE 0 END) 
        AS anomaly_score
        
        -- Score interpretation:
        -- 0: No anomalies detected
        -- 1-3: Low risk
        -- 3-5: Medium risk
        -- 5+: High risk
        
    FROM flags
)

SELECT * FROM anomaly_scoring

