SELECT v.venueid, v.venuename, SUM(qtysold) AS total_tickets_sold
FROM source_tickit.sales as s
INNER JOIN source_tickit.event as e ON s.eventid = e.eventid
INNER JOIN source_tickit.venue as v ON e.venueid = v.venueid
INNER JOIN source_tickit.category as c ON e.catid = c.catid
INNER JOIN source_tickit.date as d ON s.dateid = d.dateid
WHERE d.year = 2008
    AND c.catgroup='Concerts'
GROUP BY v.venueid, v.venuename
ORDER BY total_tickets_sold DESC
LIMIT 5;
