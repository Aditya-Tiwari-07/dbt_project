SELECT v.venue_id, v.venue_name, SUM(qty_sold) AS total_tickets_sold
FROM dbt_atiwari.fct_sales as s
INNER JOIN dbt_atiwari.dim_venues as v ON s.venue_id = v.venue_id
INNER JOIN dbt_atiwari.dim_categories as c ON s.cat_id = c.cat_id
INNER JOIN dbt_atiwari.dim_dates as d ON s.sale_date_id = d.date_id
WHERE d.year = 2008
    AND c.cat_group='Concerts'
GROUP BY v.venue_id, v.venue_name
ORDER BY total_tickets_sold DESC
LIMIT 5;
