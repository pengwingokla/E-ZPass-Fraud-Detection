

WITH  __dbt__cte___silver__cleaning as (


WITH source AS (
    SELECT * FROM `njc-ezpass`.`ezpass_data`.`bronze`
),

cleaned AS (
    SELECT
        -- Convert dates (STRING YYYY-MM-DD → DATE)
        SAFE.PARSE_DATE('%Y-%m-%d', transaction_date) as transaction_date,
        SAFE.PARSE_DATE('%Y-%m-%d', posting_date) as posting_date,
        
        -- Convert timestamps (STRING YYYY-MM-DD HH:MM:SS → TIMESTAMP)
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', entry_time) as entry_time,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', exit_time) as exit_time,
        
        -- Clean and standardize text fields (replace '-' with NULL)
        NULLIF(UPPER(TRIM(tag_plate_number)), '-') as tag_plate_number,
        NULLIF(UPPER(TRIM(agency)), '-') as agency,
        NULLIF(TRIM(description), '-') as description,
        NULLIF(UPPER(TRIM(entry_plaza)), '-') as entry_plaza,
        NULLIF(TRIM(entry_lane), '-') as entry_lane,
        NULLIF(UPPER(TRIM(exit_plaza)), '-') as exit_plaza,
        NULLIF(TRIM(exit_lane), '-') as exit_lane,
        NULLIF(TRIM(vehicle_type_code), '-') as vehicle_type_code,
        NULLIF(TRIM(plan_rate), '-') as plan_rate,
        NULLIF(TRIM(prepaid), '-') as prepaid,
        NULLIF(TRIM(fare_type), '-') as fare_type,
        
        -- Convert numeric fields (STRING → FLOAT64)
        SAFE_CAST(amount AS FLOAT64) as amount,
        SAFE_CAST(balance AS FLOAT64) as balance,
        
        -- Keep metadata
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', loaded_at) as loaded_at,
        source_file
        
    FROM source
    WHERE transaction_date IS NOT NULL  -- Filter out bad records
)

SELECT * FROM cleaned
),  __dbt__cte___silver__enrichment as (


WITH cleaned AS (
    SELECT * FROM __dbt__cte___silver__cleaning
),

enriched AS (
    SELECT
        *,
        
        -- Agency name enrichment
        CASE agency
            WHEN 'GSP' THEN 'Garden State Parkway'
            WHEN 'NJTP' THEN 'New Jersey Turnpike'
            WHEN 'SJ' THEN 'South Jersey Transportation Authority'
            WHEN 'PTC' THEN 'Pennsylvania Turnpike Commission'
            WHEN 'DRJTBC' THEN 'Delaware River Joint Toll Bridge Commission'
            WHEN 'DRPA' THEN 'Delaware River Port Authority'
            WHEN 'PANYNJ' THEN 'Port Authority of NY & NJ'
            WHEN 'BCBC' THEN 'Burlington County Bridge Commission'
            WHEN 'NJ E-ZPASS' THEN 'NJ E-ZPass (back office)'
            WHEN 'CBDTP' THEN 'Central Business District Tolling Program'
            WHEN 'DELDOT' THEN 'Delaware Department of Transportation'
            WHEN 'DRBA' THEN 'Delaware River & Bay Authority'
            WHEN 'ILTOLL' THEN 'Illinois Tollway Authority'
            WHEN 'ITRCC' THEN 'Indiana Toll Road Concession Company'
            WHEN 'MASSDOT' THEN 'Massachusetts Department of Transportation'
            WHEN 'MDTA' THEN 'Maryland Transportation Authority'
            WHEN 'META' THEN 'Maine Turnpike Authority'
            WHEN 'MTAB&T' THEN 'MTA Bridges and Tunnels'
            WHEN 'NHDOT' THEN 'New Hampshire Department of Transportation'
            WHEN 'NYSBA' THEN 'New York State Bridge Authority'
            WHEN 'NYSTA' THEN 'New York State Bridge Authority'
            WHEN 'OTIC' THEN 'Ohio Turnpike and Infrastructure Commission'
            WHEN 'VDOT' THEN 'Virginia Department of Transportation'
            ELSE NULL  -- Unknown values become NULL
        END as agency_name,

        -- State's abbreviation name
        CASE agency
            WHEN 'GSP' THEN 'NJ'
            WHEN 'NJTP' THEN 'NJ'
            WHEN 'SJ' THEN 'NJ'
            WHEN 'PTC' THEN 'PA'
            WHEN 'DRJTBC' THEN 'PA'
            WHEN 'DRPA' THEN 'PA'
            WHEN 'PANYNJ' THEN 'NY'
            WHEN 'BCBC' THEN 'PA'
            WHEN 'NJ E-ZPASS' THEN 'NJ'
            WHEN 'CBDTP' THEN 'NY'
            WHEN 'DELDOT' THEN 'DE'
            WHEN 'DRBA' THEN 'DE'
            WHEN 'ILTOLL' THEN 'IL'
            WHEN 'ITRCC' THEN 'IN'
            WHEN 'MASSDOT' THEN 'MA'
            WHEN 'MDTA' THEN 'MD'
            WHEN 'META' THEN 'ME'
            WHEN 'MTAB&T' THEN 'NY'
            WHEN 'NHDOT' THEN 'NH'
            WHEN 'NYSBA' THEN 'NY'
            WHEN 'NYSTA' THEN 'NY'
            WHEN 'OTIC' THEN 'OH'
            WHEN 'VDOT' THEN 'VA'
            ELSE NULL  -- Unknown values become NULL
        END as state_name,

        -- Entry plaza name enrichment
        CASE entry_plaza
            -- GSP (Garden State Parkway)
            WHEN 'PVK' THEN 'Pascack Valley'
            WHEN 'PRS' THEN 'Paramus South'
            WHEN 'PRN' THEN 'Paramus North'
            WHEN 'BER' THEN 'Bergen'
            WHEN 'SAB' THEN 'Saddle Brook'
            WHEN 'CLS' THEN 'Clifton South'
            WHEN 'CLN' THEN 'Clifton North'
            WHEN 'PSS' THEN 'Passaic South'
            WHEN 'PSN' THEN 'Passaic North'
            WHEN 'WAS' THEN 'Watchung South'
            WHEN 'WAN' THEN 'Watchung North'
            WHEN 'ESS' THEN 'Essex'
            WHEN 'BLS' THEN 'Bloomfield South'
            WHEN 'BLN' THEN 'Bloomfield North'
            WHEN 'EOR' THEN 'East Orange'
            WHEN 'IRS' THEN 'Irvington South'
            WHEN 'IRN' THEN 'Irvington North'
            WHEN 'UNR' THEN 'Union Ramp'
            WHEN 'UNI' THEN 'Union'
            WHEN 'RAS' THEN 'Raritan South'
            WHEN 'MAT' THEN 'Matawan'
            WHEN 'KEY' THEN 'Keyport'
            WHEN 'HOS' THEN 'Holmdel South'
            WHEN 'HON' THEN 'Holmdel North'
            WHEN 'RBS' THEN 'Red Bank South'
            WHEN 'RBN' THEN 'Red Bank North'
            WHEN 'EAT' THEN 'Eatontown'
            WHEN 'ASP' THEN 'Asbury Park'
            WHEN 'BES' THEN 'Belmar South'
            WHEN 'BEN' THEN 'Belmar North'
            WHEN 'BRS' THEN 'Brick South'
            WHEN 'BRN' THEN 'Brick North'
            WHEN 'LWS' THEN 'Lakewood South'
            WHEN 'LWN' THEN 'Lakewood North'
            WHEN 'TRV' THEN 'Toms River'
            WHEN 'LRS' THEN 'Lacey Rd South'
            WHEN 'LRN' THEN 'Lacey Rd North'
            WHEN 'BAR' THEN 'Barnegat'
            WHEN 'BKS' THEN 'Berkeley Ramp South'
            WHEN 'BKN' THEN 'Berkeley Ramp North'
            WHEN 'NGR' THEN 'New Gretna'
            WHEN 'WRS' THEN 'Waretown South'
            WHEN 'WRN' THEN 'Waretown North'
            WHEN 'SPT' THEN 'Somers Point'
            WHEN 'GEG' THEN 'Great Egg'
            WHEN 'CMY' THEN 'Cape May'
            WHEN 'WWS' THEN 'Wildwood South'
            WHEN 'WWN' THEN 'Wildwood North'
            WHEN 'SAY' THEN 'Sayreville'
            -- NJTP (Turnpike)
            WHEN '1' THEN 'Delaware Memorial Bridge'
            WHEN '2' THEN 'Swedesboro/Chester'
            WHEN '3' THEN 'Woodbury/S. Camden/NJ Aquarium'
            WHEN '4' THEN 'Camden/Philadelphia/NJ Aquarium'
            WHEN '5' THEN 'Burlington/Mt. Holly'
            WHEN '6' THEN 'PA Turnpike/Florence'
            WHEN '6A' THEN 'PA Turnpike/Florence'
            WHEN '6B' THEN 'Rte. 130 Credit Ramp'
            WHEN '7' THEN 'Bordentown/Trenton'
            WHEN '7A' THEN 'I-195/Trenton/Shore Points'
            WHEN '8' THEN 'Hightstown/Freehold'
            WHEN '8A' THEN 'Jamesburg/Cranbury'
            WHEN '9' THEN 'New Brunswick/Admin Bldg'
            WHEN '10' THEN 'I-287/Metuchen/Edison Twsp'
            WHEN '11' THEN 'GSP/Woodbridge/The Amboys'
            WHEN '12' THEN 'Carteret/Rahway'
            WHEN '13' THEN 'I-278/Eliz/Goethals/Verrazano'
            WHEN '13A' THEN 'Newark Aprt/Elizabeth Seaport'
            WHEN '14' THEN 'I-78/Newark Airport'
            WHEN '14A' THEN 'Bayonne'
            WHEN '14B' THEN 'Jersey City/Liberty State Park'
            WHEN '14C' THEN 'Holland Tunnel'
            WHEN '15E' THEN 'Newark/Jersey City'
            WHEN '15W' THEN 'I-280/Newark/The Oranges'
            WHEN '15X' THEN 'Secaucus Transfer Station'
            WHEN '16E' THEN 'Lincoln Tunnel/NJ 3/Secaucus'
            WHEN '16W' THEN 'Sprtsplx/NJ 3/Secaucus/Ruthrfrd'
            WHEN '17' THEN 'Secaucus/US 46'
            WHEN '18E' THEN 'Lincoln Tunnel/NJ 3/Secaucus'
            WHEN '18W' THEN 'Geo Washington Br/US 46/I-80'
            WHEN '19W' THEN 'Carlstadt'
            -- SJ (South Jersey)
            WHEN 'APL' THEN 'Pleasantville Mainline Barrier'
            WHEN 'AR9' THEN 'Route 9'
            WHEN 'APO' THEN 'Pomona'
            WHEN 'ACY' THEN 'AC Airport'
            WHEN 'AML' THEN 'Mays Landing'
            WHEN 'A50' THEN 'Route 50'
            WHEN 'AEH' THEN 'Egg Harbor Mainline Barrier'
            WHEN 'AH' THEN 'Hammonton Ramp'
            WHEN 'AWN' THEN 'Winslow Ramp'
            WHEN 'AWL' THEN 'Williamstown Ramp'
            WHEN 'ACK' THEN 'Cross Keys'
            WHEN 'OCL' THEN 'Ocean City-Longport Bridge'
            WHEN 'CIB' THEN 'Corsons Inlet Bridge'
            WHEN 'TIB' THEN 'Townsends Inlet Bridge'
            WHEN 'GSB' THEN 'Grassy Sound Bridge'
            WHEN 'MTB' THEN 'Middle Thorofare Bridge'
            -- DRPA
            WHEN 'BRB' THEN 'Betsy Ross Br'
            WHEN 'BFB' THEN 'Ben Franklin Br'
            WHEN 'WWB' THEN 'Walt Whitman Br'
            WHEN 'CBB' THEN 'Commodore Barry Br'
            -- DRBA
            WHEN 'DMB' THEN 'Delaware Memorial Br'
            -- BCBC
            WHEN 'TPB' THEN 'Tacony Palmyra Br'
            WHEN 'BBB' THEN 'Burlington Bristol Br'
            -- DRJTBC
            WHEN 'T-M' THEN 'Trenton-Morrisville Br'
            WHEN 'NHL' THEN 'New Hope-Lambertville Br'
            WHEN 'I78' THEN 'I-78 Br'
            WHEN 'E-P' THEN 'Easton-Phillipsburg Br'
            WHEN 'P-C' THEN 'Portland-Columbia Br'
            WHEN 'DWG' THEN 'Delaware Water Gap Br'
            WHEN 'M-M' THEN 'Milford-Montague Br'
            WHEN 'O78' THEN 'Interstate 78-ORT'
            WHEN 'ODW' THEN 'Delaware Water Gap-ORT'
            WHEN 'OSF' THEN 'Scudder Falls Br'
            ELSE NULL  -- Unknown values become NULL
        END as entry_plaza_name,
        
        -- Exit plaza name enrichment
        CASE exit_plaza
            -- GSP
            WHEN 'PVK' THEN 'Pascack Valley'
            WHEN 'PRS' THEN 'Paramus South'
            WHEN 'PRN' THEN 'Paramus North'
            WHEN 'BER' THEN 'Bergen'
            WHEN 'SAB' THEN 'Saddle Brook'
            WHEN 'CLS' THEN 'Clifton South'
            WHEN 'CLN' THEN 'Clifton North'
            WHEN 'PSS' THEN 'Passaic South'
            WHEN 'PSN' THEN 'Passaic North'
            WHEN 'WAS' THEN 'Watchung South'
            WHEN 'WAN' THEN 'Watchung North'
            WHEN 'ESS' THEN 'Essex'
            WHEN 'BLS' THEN 'Bloomfield South'
            WHEN 'BLN' THEN 'Bloomfield North'
            WHEN 'EOR' THEN 'East Orange'
            WHEN 'IRS' THEN 'Irvington South'
            WHEN 'IRN' THEN 'Irvington North'
            WHEN 'UNR' THEN 'Union Ramp'
            WHEN 'UNI' THEN 'Union'
            WHEN 'RAS' THEN 'Raritan South'
            WHEN 'MAT' THEN 'Matawan'
            WHEN 'KEY' THEN 'Keyport'
            WHEN 'HOS' THEN 'Holmdel South'
            WHEN 'HON' THEN 'Holmdel North'
            WHEN 'RBS' THEN 'Red Bank South'
            WHEN 'RBN' THEN 'Red Bank North'
            WHEN 'EAT' THEN 'Eatontown'
            WHEN 'ASP' THEN 'Asbury Park'
            WHEN 'BES' THEN 'Belmar South'
            WHEN 'BEN' THEN 'Belmar North'
            WHEN 'BRS' THEN 'Brick South'
            WHEN 'BRN' THEN 'Brick North'
            WHEN 'LWS' THEN 'Lakewood South'
            WHEN 'LWN' THEN 'Lakewood North'
            WHEN 'TRV' THEN 'Toms River'
            WHEN 'LRS' THEN 'Lacey Rd South'
            WHEN 'LRN' THEN 'Lacey Rd North'
            WHEN 'BAR' THEN 'Barnegat'
            WHEN 'BKS' THEN 'Berkeley Ramp South'
            WHEN 'BKN' THEN 'Berkeley Ramp North'
            WHEN 'NGR' THEN 'New Gretna'
            WHEN 'WRS' THEN 'Waretown South'
            WHEN 'WRN' THEN 'Waretown North'
            WHEN 'SPT' THEN 'Somers Point'
            WHEN 'GEG' THEN 'Great Egg'
            WHEN 'CMY' THEN 'Cape May'
            WHEN 'WWS' THEN 'Wildwood South'
            WHEN 'WWN' THEN 'Wildwood North'
            WHEN 'SAY' THEN 'Sayreville'
            -- NJTP
            WHEN '1' THEN 'Delaware Memorial Bridge'
            WHEN '2' THEN 'Swedesboro/Chester'
            WHEN '3' THEN 'Woodbury/S. Camden/NJ Aquarium'
            WHEN '4' THEN 'Camden/Philadelphia/NJ Aquarium'
            WHEN '5' THEN 'Burlington/Mt. Holly'
            WHEN '6' THEN 'PA Turnpike/Florence'
            WHEN '6A' THEN 'PA Turnpike/Florence'
            WHEN '6B' THEN 'Rte. 130 Credit Ramp'
            WHEN '7' THEN 'Bordentown/Trenton'
            WHEN '7A' THEN 'I-195/Trenton/Shore Points'
            WHEN '8' THEN 'Hightstown/Freehold'
            WHEN '8A' THEN 'Jamesburg/Cranbury'
            WHEN '9' THEN 'New Brunswick/Admin Bldg'
            WHEN '10' THEN 'I-287/Metuchen/Edison Twsp'
            WHEN '11' THEN 'GSP/Woodbridge/The Amboys'
            WHEN '12' THEN 'Carteret/Rahway'
            WHEN '13' THEN 'I-278/Eliz/Goethals/Verrazano'
            WHEN '13A' THEN 'Newark Aprt/Elizabeth Seaport'
            WHEN '14' THEN 'I-78/Newark Airport'
            WHEN '14A' THEN 'Bayonne'
            WHEN '14B' THEN 'Jersey City/Liberty State Park'
            WHEN '14C' THEN 'Holland Tunnel'
            WHEN '15E' THEN 'Newark/Jersey City'
            WHEN '15W' THEN 'I-280/Newark/The Oranges'
            WHEN '15X' THEN 'Secaucus Transfer Station'
            WHEN '16E' THEN 'Lincoln Tunnel/NJ 3/Secaucus'
            WHEN '16W' THEN 'Sprtsplx/NJ 3/Secaucus/Ruthrfrd'
            WHEN '17' THEN 'Secaucus/US 46'
            WHEN '18E' THEN 'Lincoln Tunnel/NJ 3/Secaucus'
            WHEN '18W' THEN 'Geo Washington Br/US 46/I-80'
            WHEN '19W' THEN 'Carlstadt'
            -- SJ
            WHEN 'APL' THEN 'Pleasantville Mainline Barrier'
            WHEN 'AR9' THEN 'Route 9'
            WHEN 'APO' THEN 'Pomona'
            WHEN 'ACY' THEN 'AC Airport'
            WHEN 'AML' THEN 'Mays Landing'
            WHEN 'A50' THEN 'Route 50'
            WHEN 'AEH' THEN 'Egg Harbor Mainline Barrier'
            WHEN 'AH' THEN 'Hammonton Ramp'
            WHEN 'AWN' THEN 'Winslow Ramp'
            WHEN 'AWL' THEN 'Williamstown Ramp'
            WHEN 'ACK' THEN 'Cross Keys'
            WHEN 'OCL' THEN 'Ocean City-Longport Bridge'
            WHEN 'CIB' THEN 'Corsons Inlet Bridge'
            WHEN 'TIB' THEN 'Townsends Inlet Bridge'
            WHEN 'GSB' THEN 'Grassy Sound Bridge'
            WHEN 'MTB' THEN 'Middle Thorofare Bridge'
            -- DRPA
            WHEN 'BRB' THEN 'Betsy Ross Br'
            WHEN 'BFB' THEN 'Ben Franklin Br'
            WHEN 'WWB' THEN 'Walt Whitman Br'
            WHEN 'CBB' THEN 'Commodore Barry Br'
            -- DRBA
            WHEN 'DMB' THEN 'Delaware Memorial Br'
            -- BCBC
            WHEN 'TPB' THEN 'Tacony Palmyra Br'
            WHEN 'BBB' THEN 'Burlington Bristol Br'
            -- DRJTBC
            WHEN 'T-M' THEN 'Trenton-Morrisville Br'
            WHEN 'NHL' THEN 'New Hope-Lambertville Br'
            WHEN 'I78' THEN 'I-78 Br'
            WHEN 'E-P' THEN 'Easton-Phillipsburg Br'
            WHEN 'P-C' THEN 'Portland-Columbia Br'
            WHEN 'DWG' THEN 'Delaware Water Gap Br'
            WHEN 'M-M' THEN 'Milford-Montague Br'
            WHEN 'O78' THEN 'Interstate 78-ORT'
            WHEN 'ODW' THEN 'Delaware Water Gap-ORT'
            WHEN 'OSF' THEN 'Scudder Falls Br'
            ELSE NULL  -- Unknown values become NULL
        END as exit_plaza_name
        
    FROM cleaned
)

SELECT * FROM enriched
), enriched AS (
    SELECT * FROM __dbt__cte___silver__enrichment
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
            WHEN vehicle_type_code IN ('1', '2', '2L', '2H') THEN 'Passenger Vehicle'
            WHEN vehicle_type_code IN ('3', '3L', '3H', 'B2', 'B3') THEN 'Light Commercial'
            WHEN vehicle_type_code IN ('4', '5', '6', '7', '8', '9', '4L', '4H', '5H', '6H', '7H') THEN 'Heavy Commercial'
            ELSE 'Unknown'
        END as vehicle_class_category,
        
        -- Daily Number of Transactions by tag_plate_number
        COUNT(*) OVER (PARTITION BY tag_plate_number, transaction_date) AS daily_count

    FROM enriched
)

SELECT * FROM new_features