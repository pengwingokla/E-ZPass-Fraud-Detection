{{ config(
    materialized='ephemeral',
    tags=['silver', 'intermediate']
) }}

WITH base_features AS (
    SELECT * FROM {{ ref('_silver__feateng_route') }}
),

price_features AS (
    SELECT
        *,
        
        -- Rolling average toll amount over last 30 transactions per driver
        AVG(amount) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY COALESCE(entry_time, TIMESTAMP('1900-01-01 00:00:00'))
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as driver_amount_last_30txn_avg,
        
        -- Rolling standard deviation of toll amounts over last 30 transactions per driver
        STDDEV(amount) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY COALESCE(entry_time, TIMESTAMP('1900-01-01 00:00:00'))
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as driver_amount_last_30txn_std,
        
        -- Rolling minimum toll amount over last 30 transactions per driver
        MIN(amount) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY COALESCE(entry_time, TIMESTAMP('1900-01-01 00:00:00'))
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as driver_amount_last_30txn_min,
        
        -- Rolling maximum toll amount over last 30 transactions per driver
        MAX(amount) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY COALESCE(entry_time, TIMESTAMP('1900-01-01 00:00:00'))
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as driver_amount_last_30txn_max,
        
        -- Rolling count of transactions (useful for normalization)
        COUNT(*) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY COALESCE(entry_time, TIMESTAMP('1900-01-01 00:00:00'))
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as driver_amount_last_30txn_count

    FROM base_features
),

-- Calculate route-level statistics using window functions
route_amount_base AS (
    SELECT
        pf.*,
        AVG(pf.amount) OVER (
            PARTITION BY pf.route_name, pf.vehicle_type_code
        ) as route_amount_avg,
        STDDEV(pf.amount) OVER (
            PARTITION BY pf.route_name, pf.vehicle_type_code
        ) as route_amount_std,
        MIN(pf.amount) OVER (
            PARTITION BY pf.route_name, pf.vehicle_type_code
        ) as route_amount_min,
        MAX(pf.amount) OVER (
            PARTITION BY pf.route_name, pf.vehicle_type_code
        ) as route_amount_max,
        PERCENTILE_CONT(amount, 0.5) OVER (
            PARTITION BY pf.route_name, pf.vehicle_type_code
        ) as route_amount_med_raw,
        COUNT(*) OVER (
            PARTITION BY pf.route_name, pf.vehicle_type_code
        ) as route_transaction_count
    FROM price_features pf
),

-- Only keep median if route has enough transactions (30+)
route_amount AS (
    SELECT
        * EXCEPT(route_amount_med_raw),
        -- Only use median if route has enough transactions (30+)
        CASE 
            WHEN route_transaction_count >= 30 THEN route_amount_med_raw
            ELSE NULL
        END as route_amount_med
    FROM route_amount_base
)

SELECT * FROM route_amount