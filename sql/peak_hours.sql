SELECT
    pickup_hour,
    COUNT(*) AS total_trips
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY total_trips DESC, pickup_hour