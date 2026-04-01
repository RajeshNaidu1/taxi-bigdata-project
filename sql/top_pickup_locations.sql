SELECT
    PULocationID,
    COUNT(*) AS total_trips
FROM taxi_trips
GROUP BY PULocationID
ORDER BY total_trips DESC
LIMIT 10