{{ config(
    materialized='ephemeral',
    tags=['gold', 'base']
) }}

WITH source AS (
    SELECT * FROM {{ ref('silver') }}
),

-- Calculate historical median for each driver (all-time, not rolling)
driver_historical_medians AS (
    SELECT
        tag_plate_number,
        APPROX_QUANTILES(amount, 100)[OFFSET(50)] as driver_amount_median
    FROM source
    GROUP BY tag_plate_number
),

driver_features AS (
    SELECT
        s.*,
        dm.driver_amount_median,
        
        -- Z-score: How many standard deviations is this transaction from driver's historical median
        SAFE_DIVIDE(
            s.amount - dm.driver_amount_median, -- medians are more robust to outliers than averages
            NULLIF(s.driver_amount_last_30txn_std, 0)
        ) as driver_amount_modified_z_score,
        
        -- Percentage deviation from driver's average
        SAFE_DIVIDE(
            (s.amount - s.driver_amount_last_30txn_avg),
            NULLIF(s.driver_amount_last_30txn_avg, 0)
        ) * 100 as amount_deviation_from_avg_pct,
        
        -- Ratio to historical median
        SAFE_DIVIDE(
            (s.amount - dm.driver_amount_median),
            NULLIF(dm.driver_amount_median, 0)
        ) as amount_deviation_from_median_pct

    FROM source s
    LEFT JOIN driver_historical_medians dm
        ON s.tag_plate_number = dm.tag_plate_number
),

route_features AS (
    SELECT
        *,
        
        -- Z-score: How many standard deviations is this transaction from driver's average
        -- already paritioned by vehicle type code in silver layer
        SAFE_DIVIDE(
            amount - route_amount_avg,
            NULLIF(route_amount_std, 0)
        ) as route_amount_z_score

    FROM driver_features
),

-- Pre-calculate daily spend per driver per date
daily_spend_aggregated AS (
    SELECT
        tag_plate_number,
        transaction_date,
        SUM(amount) as driver_today_spend
    FROM route_features
    GROUP BY tag_plate_number, transaction_date
),

-- Calculate 30-day rolling average of daily spend
daily_spend_with_avg AS (
    SELECT
        *,
        AVG(driver_today_spend) OVER (
            PARTITION BY tag_plate_number
            ORDER BY UNIX_DATE(transaction_date)
            RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as driver_avg_daily_spend_30d
    FROM daily_spend_aggregated
),

-- Join daily spend features back to transactions
daily_spend_features AS (
    SELECT
        rf.*,
        ds.driver_today_spend,
        ds.driver_avg_daily_spend_30d
    FROM route_features rf
    LEFT JOIN daily_spend_with_avg ds
        ON rf.tag_plate_number = ds.tag_plate_number
        AND rf.transaction_date = ds.transaction_date
)

SELECT * FROM daily_spend_features

