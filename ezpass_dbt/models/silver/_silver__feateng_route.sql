{{ config(
    materialized='ephemeral',
    tags=['silver', 'intermediate']
) }}

WITH base_features AS (
    SELECT * FROM {{ ref('_silver__feateng') }}
),


distance_lookup AS (
    SELECT * FROM {{ source('raw', 'address_to_miles') }}
),

-- Extract all in-state plazas (plazas that appear in origin_plaza column)
instate_plazas AS (
    SELECT DISTINCT origin_plaza as plaza_name
    FROM distance_lookup
),

route_sequence AS (
    SELECT
        *,
        
        -- Previous entry plaza
        LAG(entry_plaza) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY exit_time ASC NULLS LAST
        ) as entry_plaza_previous,
        
        -- Previous exit plaza
        LAG(exit_plaza) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY exit_time ASC NULLS LAST
        ) as exit_plaza_previous,
        
        -- Previous entry time
        LAG(entry_time) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY exit_time ASC NULLS LAST
        ) as entry_time_previous,
        
        -- Previous exit time
        LAG(exit_time) OVER (
            PARTITION BY tag_plate_number 
            ORDER BY exit_time ASC NULLS LAST
        ) as exit_time_previous

    FROM base_features
),

-- Build route string with cleaned plazas
route_features AS (
    SELECT
        *,
        
        -- Route from previous exit to current exit
        CONCAT(
            COALESCE(exit_plaza_previous, 'Unknown'), 
            ' to ', 
            exit_plaza
        ) as route_name

    FROM route_sequence
),

-- Add route classification features
check_route AS (
    SELECT
        rf.*,

        CASE
            -- If either plaza is Unknown or NULL, route classification is Unknown
            WHEN rf.exit_plaza IS NULL OR rf.exit_plaza = 'Unknown' 
                 OR rf.exit_plaza_previous IS NULL OR rf.exit_plaza_previous = 'Unknown' 
            THEN 'Unknown'
            -- Both plazas must be in-state for route to be In-state
            WHEN rf.exit_plaza IN (SELECT plaza_name FROM instate_plazas)
                 AND rf.exit_plaza_previous IN (SELECT plaza_name FROM instate_plazas)
            THEN 'In-state'
            ELSE 'Out-state'
        END as route_instate
        
    FROM route_features rf
),

-- Add distance from previous exit plaza to current exit plaza
distance_features AS (
    SELECT
        cr.*,
        dl.expected_travel_distance_miles as distance_miles
    FROM check_route cr
    LEFT JOIN distance_lookup dl
        ON cr.exit_plaza_previous = dl.origin_plaza
        AND cr.exit_plaza = dl.destination_plaza
),

-- Calculate velocity and impossible travel features
velocity_features AS (
    SELECT
        *,
        
        -- Time since last transaction (in minutes)
        TIMESTAMP_DIFF(
            exit_time, 
            exit_time_previous, -- this is the entry time
            MINUTE
        ) as travel_time_minutes,
        
        -- Calculate implied speed (mph)
        -- Only calculate if we have valid distance and time data
        CASE 
            WHEN distance_miles IS NOT NULL 
                 AND exit_time IS NOT NULL 
                 AND exit_time_previous IS NOT NULL
                 AND TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) > 0
            THEN (distance_miles / TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE)) * 60
            ELSE NULL
        END as speed_mph,
        
        -- Minimum required travel time at reasonable speed (88 mph)
        CASE 
            WHEN distance_miles IS NOT NULL 
                 AND distance_miles > 0
            THEN (distance_miles / 88.0) * 60  -- Convert hours to minutes
            ELSE NULL
        END as min_required_travel_time_minutes,
        
        -- Flag for impossible travel (speed > 100 mph OR time less than required OR negative time)
        CASE 
            WHEN distance_miles IS NULL 
                OR exit_time IS NULL 
                OR exit_time_previous IS NULL
            THEN NULL
            -- Check for zero or negative time difference (impossible - time travel or out of order)
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) <= 0
            THEN TRUE
            -- Check if travel time is less than physically possible at 100 mph
            WHEN distance_miles IS NOT NULL 
                 AND distance_miles > 0
                 AND TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) < (distance_miles / 90.0) * 60
            THEN TRUE
            -- Check if implied speed exceeds 100 mph
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) > 0
                 AND (distance_miles / TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE)) * 60 >= 100
            THEN TRUE
            ELSE FALSE
        END as is_impossible_travel,
        
        -- Flag for rapid succession (< 5 minutes between transactions)
        CASE 
            WHEN exit_time IS NULL OR exit_time_previous IS NULL
            THEN NULL
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) < 5
            THEN TRUE
            ELSE FALSE
        END as is_rapid_succession,
        
        -- Flag for overlapping transactions (entry time before previous exit time)
        CASE
            WHEN entry_time IS NOT NULL
                AND exit_time_previous IS NOT NULL
                AND entry_time < exit_time_previous
            THEN TRUE -- TRUE if this transaction's entry time started before previous transaction's exit time
            WHEN entry_time IS NOT NULL 
                AND exit_time_previous IS NOT NULL
            THEN FALSE -- FALSE if both times exist but there's no overlap
            ELSE FALSE -- FALSE/null? if either entry time or exit time previous is NULL
        END as flag_overlapping_journey,
        
        -- Absolute overlap duration
        CASE 
            WHEN entry_time < exit_time_previous
            THEN TIMESTAMP_DIFF(exit_time_previous, entry_time, MINUTE)
            ELSE 0
        END as overlapping_journey_duration_minutes,
        
        -- Categorize travel time
        CASE 
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) IS NULL THEN NULL
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) < 0 THEN 'Invalid (Negative)'
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) < 10 THEN 'Short (<10 min)'
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) < 30 THEN 'Medium (10-30 min)'
            WHEN TIMESTAMP_DIFF(exit_time, exit_time_previous, MINUTE) < 60 THEN 'Long (30-60 min)'
            ELSE 'Very Long (60+ min)'
        END as travel_time_category

    FROM distance_features
)


SELECT * FROM velocity_features

