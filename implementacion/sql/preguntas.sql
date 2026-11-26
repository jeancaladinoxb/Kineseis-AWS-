-- Q1 Ingreso neto por año, mes y clase tarifaria.
SELECT d.year, d.month, c.fare_class, SUM(b.revenue_cents) AS net_revenue_cents
FROM fact_booking_event b JOIN dim_date d ON d.date_key=b.date_key
JOIN dim_fare_class c ON c.fare_class_key=b.fare_class_key
GROUP BY 1,2,3 ORDER BY 1,2,3;

-- Q2 Reservas, cancelaciones y tasa por ruta.
SELECT r.route, SUM(b.bookings) AS bookings, SUM(b.cancellations) AS cancellations,
       CAST(SUM(b.cancellations) AS DOUBLE)/NULLIF(SUM(b.bookings),0) AS cancellation_rate
FROM fact_booking_event b JOIN dim_route r ON b.route_key=r.route_key
GROUP BY 1 ORDER BY 1;

-- Q3 Reservas por día de la semana, en orden de lunes a domingo.
SELECT d.weekday, d.weekday_name, SUM(b.bookings) AS bookings
FROM fact_booking_event b JOIN dim_date d ON d.date_key=b.date_key
GROUP BY 1,2 ORDER BY 1;

-- Q4 Diez rutas de mayor ingreso neto.
SELECT r.route, SUM(b.revenue_cents) AS net_revenue_cents
FROM fact_booking_event b JOIN dim_route r ON b.route_key=r.route_key
GROUP BY 1 ORDER BY 2 DESC,1 LIMIT 10;

-- Q5 Ingreso mensual según la categoría vigente al ocurrir el evento.
SELECT d.year, d.month, r.route_category, SUM(b.revenue_cents) AS net_revenue_cents
FROM fact_booking_event b JOIN dim_date d ON b.date_key=d.date_key
JOIN dim_route r ON r.route_key=b.route_key GROUP BY 1,2,3 ORDER BY 1,2,3;

-- Q6 Conversión. Agregar cada hecho antes de unir evita multiplicar filas.
WITH counts AS (
 SELECT r.route, SUM(s.searches) AS searches, 0 AS bookings
 FROM fact_search s JOIN dim_route r ON r.route_key=s.route_key GROUP BY 1
 UNION ALL
 SELECT r.route, 0, SUM(b.bookings)
 FROM fact_booking_event b JOIN dim_route r ON r.route_key=b.route_key GROUP BY 1
)
SELECT route, SUM(searches) AS searches, SUM(bookings) AS bookings,
       CAST(SUM(bookings) AS DOUBLE)/NULLIF(SUM(searches),0) AS conversion
FROM counts GROUP BY 1 ORDER BY 1;

-- Q7 Búsquedas y reservas en festivos y otros días.
WITH counts AS (
 SELECT d.is_holiday, SUM(s.searches) AS searches, 0 AS bookings
 FROM fact_search s JOIN dim_date d ON s.date_key=d.date_key GROUP BY 1
 UNION ALL
 SELECT d.is_holiday, 0, SUM(b.bookings)
 FROM fact_booking_event b JOIN dim_date d ON b.date_key=d.date_key GROUP BY 1
)
SELECT is_holiday, SUM(searches) AS searches, SUM(bookings) AS bookings
FROM counts GROUP BY 1 ORDER BY 1;

-- Q8 Reservas en días de puente frente al resto del calendario.
SELECT d.is_long_weekend, SUM(b.bookings) AS bookings
FROM fact_booking_event b JOIN dim_date d ON d.date_key=b.date_key
GROUP BY 1 ORDER BY 1;
