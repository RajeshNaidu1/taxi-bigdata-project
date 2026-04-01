SELECT
    payment_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM taxi_trips
GROUP BY payment_type
ORDER BY total_trips DESC