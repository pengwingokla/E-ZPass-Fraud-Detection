{{ config(
    materialized='ephemeral',
    tags=['gold', 'base']
) }}

WITH aggregated_features AS (
    SELECT * FROM {{ ref('_gold__agg') }}
),

driver_flags AS (
    SELECT
        *,
        CASE WHEN driver_amount_modified_z_score > 1 THEN TRUE ELSE FALSE 
        END as flag_driver_amount_outlier,

        CASE WHEN ABS(route_amount_z_score) > 1 THEN TRUE ELSE FALSE 
        END as flag_route_amount_outlier,
        
        CASE WHEN amount_deviation_from_avg_pct > 80 
            OR amount_deviation_from_median_pct > 80 THEN TRUE ELSE FALSE -- 80% deviation from average or median
        END as flag_amount_unusually_high,

        -- Could add flag_unusually_low here if needed
        
        -- Flag for spending spike (daily spend > 3x average and amount >= $20)
        CASE 
            WHEN driver_avg_daily_spend_30d IS NOT NULL 
                AND (
                    driver_today_spend > (3 * driver_avg_daily_spend_30d)
                    OR driver_today_spend = driver_amount_last_30txn_max
                )
                AND amount >= 50
            THEN TRUE
            ELSE FALSE
        END as flag_driver_spend_spike
        
    FROM aggregated_features
)

SELECT * FROM driver_flags

