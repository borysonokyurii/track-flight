SELECT
    flight_id,
    delay_minutes
FROM {{ ref('fct_flights_weather') }}
WHERE delay_minutes < -1440 OR delay_minutes > 1440
