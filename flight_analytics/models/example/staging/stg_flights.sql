{{ config(
    materialized='incremental',
    unique_key='flight_id'
) }}

WITH raw_json AS (
    SELECT
        extracted_date,
        jsonb_array_elements(raw_data) AS flight_node
    FROM {{ source('raw_data', 'raw_flights') }}
    
    {% if is_incremental() %}
    WHERE extracted_date >= (SELECT COALESCE(MAX(extracted_date), '1900-01-01'::DATE) FROM {{ this }})
    {% endif %}
),

parsed_flights AS (
    SELECT
        extracted_date,
        (flight_node ->> 'id') AS flight_id,
        (flight_node ->> 'flightName') AS flight_name,
        (flight_node ->> 'prefixIATA') AS airline_iata,
        
        (flight_node ->> 'scheduleDate')::DATE AS schedule_date,
        (flight_node ->> 'scheduleTime')::TIME AS schedule_time,
        
        (flight_node ->> 'actualLandingTime')::TIMESTAMP AS actual_landing_time,
        (flight_node ->> 'estimatedLandingTime')::TIMESTAMP AS estimated_landing_time,
        
        (flight_node ->> 'publicFlightState') AS flight_state_json
    FROM raw_json
),

deduplicated_flights AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY flight_id ORDER BY extracted_date DESC) as rn
    FROM parsed_flights
    WHERE flight_id IS NOT NULL
)

SELECT 
    extracted_date,
    flight_id,
    flight_name,
    airline_iata,
    schedule_date,
    schedule_time,
    actual_landing_time,
    estimated_landing_time,
    flight_state_json
FROM deduplicated_flights
WHERE rn = 1