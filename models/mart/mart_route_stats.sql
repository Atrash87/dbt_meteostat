WITH route_stats AS (
    SELECT 
        origin,
        dest,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines,
        ROUND(AVG(actual_elapsed_time), 2) AS avg_act_elapsed_time,
        ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
        MAX(arr_delay) AS max_arr_delay,
        MIN(arr_delay) AS min_arr_delay,
        SUM(cancelled) AS total_cancelled,
        SUM(diverted) AS total_diverted
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, dest
),

route_info AS (
    SELECT 
        rs.*,
        ao.city AS origin_city,
        ao.country AS origin_country,
        ao.name AS origin_name,
        ad.city AS dest_city,
        ad.country AS dest_country,
        ad.name AS dest_name
    FROM route_stats rs
    LEFT JOIN {{ ref('prep_airports') }} ao ON rs.origin = ao.faa
    LEFT JOIN {{ ref('prep_airports') }} ad ON rs.dest = ad.faa
)

SELECT * FROM route_info
