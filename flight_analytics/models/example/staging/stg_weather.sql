{{ config(
    materialized='incremental',
    unique_key='weather_time'
) }}

WITH raw_weather AS (
    SELECT
        extracted_date,
        raw_data
    FROM {{ source('raw_data', 'raw_weather') }}
    
    {% if is_incremental() %}
    WHERE extracted_date >= (SELECT COALESCE(MAX(extracted_date), '1900-01-01'::DATE) FROM {{ this }})
    {% endif %}
),

unnested_weather AS (
    SELECT 
        extracted_date,
        (raw_data -> 'hourly' -> 'time' ->> idx)::TIMESTAMP AS weather_time,
        (raw_data -> 'hourly' -> 'temperature_2m' ->> idx)::FLOAT AS temperature_celsius,
        (raw_data -> 'hourly' -> 'wind_speed_10m' ->> idx)::FLOAT AS wind_speed_kmh,
        (raw_data -> 'hourly' -> 'precipitation' ->> idx)::FLOAT AS precipitation_mm,
        (raw_data -> 'hourly' -> 'weather_code' ->> idx)::INT AS weather_code
    FROM raw_weather,
    jsonb_array_elements(raw_data -> 'hourly' -> 'time') WITH ORDINALITY AS arr(elem, idx_1)
    CROSS JOIN LATERAL (SELECT (idx_1 - 1)::int AS idx) AS indices
)

SELECT * FROM unnested_weather