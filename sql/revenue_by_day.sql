SELECT
    pickup_date,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM taxi_trips
GROUP BY pickup_date
ORDER BY pickup_date