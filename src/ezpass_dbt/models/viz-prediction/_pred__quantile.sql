{{ config(
    materialized='ephemeral',
    tags=['predictions', 'quantiles']
) }}

WITH predictions AS (
    SELECT 
        *
    FROM {{ source('ezpass_predictions', 'fraud_predictions') }}
),

-- Calculate percentiles for anomaly score distribution
-- Note: Lower (more negative) scores = more anomalous
score_percentiles AS (
    SELECT
        APPROX_QUANTILES(ml_anomaly_score, 100)[OFFSET(1)] AS p1,   -- 1st percentile (most anomalous)
        APPROX_QUANTILES(ml_anomaly_score, 100)[OFFSET(5)] AS p5,   -- 5th percentile
        APPROX_QUANTILES(ml_anomaly_score, 100)[OFFSET(25)] AS p25  -- 25th percentile
    FROM predictions
),

-- Categorize each transaction based on percentiles
categorized AS (
    SELECT
        p.*,
        sp.p1,
        sp.p5,
        sp.p25,
        
        -- Assign risk category based on score percentiles
        -- Lower (more negative) scores = higher risk
        CASE
            WHEN p.ml_anomaly_score <= sp.p1 THEN 'Critical Risk'   -- Bottom 1% (most anomalous)
            WHEN p.ml_anomaly_score <= sp.p5 THEN 'Medium Risk'       -- Bottom 5%
            WHEN p.ml_anomaly_score <= sp.p25 THEN 'Low Risk'    -- Bottom 25%
            ELSE 'No Risk'                                          -- Top 75%
        END AS anomaly_category,
        
        -- Add percentile rank for each transaction (lower rank = more anomalous)
        PERCENT_RANK() OVER (ORDER BY p.ml_anomaly_score ASC) AS score_percentile_rank
        
    FROM predictions p
    CROSS JOIN score_percentiles sp
)

SELECT * FROM categorized

