{{ config(
    materialized='incremental',
    unique_key='flight_id'
) }}

WITH flights AS (
    SELECT
        flight_id,
        flight_name,
        airline_iata,
        schedule_date,
        schedule_time,
        (schedule_date + schedule_time) AS scheduled_timestamp,
        actual_landing_time,
        
        date_trunc('hour', actual_landing_time) AS join_weather_time,
        
        ROUND(
            (EXTRACT(EPOCH FROM (actual_landing_time - (schedule_date + schedule_time))) / 60)::NUMERIC, 
            0
        ) AS delay_minutes
        
    FROM {{ ref('stg_flights') }}
    WHERE actual_landing_time IS NOT NULL
    
    {% if is_incremental() %}
      AND actual_landing_time >= (SELECT COALESCE(MAX(actual_landing_time), '1900-01-01'::TIMESTAMP) FROM {{ this }})
    {% endif %}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
)

SELECT
    f.flight_id,
    f.flight_name,
    f.airline_iata,
    f.scheduled_timestamp,
    f.actual_landing_time,
    f.delay_minutes,
    
    CASE 
        WHEN f.delay_minutes > 15 THEN true 
        ELSE false 
    END AS is_delayed,
    
    w.temperature_celsius,
    w.wind_speed_kmh,
    w.precipitation_mm,
    w.weather_code,
    CASE 
        WHEN w.weather_code IN (45, 48) THEN true 
        ELSE false 
    END AS is_foggy

FROM flights f
LEFT JOIN weather w
    ON f.join_weather_time = w.weather_time