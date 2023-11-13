{{ config(materialized="view", bind=False) }}

with
    sales as (select * from {{ ref('stg_sales') }}),

    events as (select * from {{ ref('stg_events') }}),

    venues as (select * from {{ ref('stg_venues') }}),

    final as (

        select distinct
            v.venue_id,
            v.venue_name,
            v.venue_city,
            v.venue_state,
            v.venue_seats,
            min (s.ticket_price) as min_ticket_price,
            max (s.ticket_price) as max_ticket_price,
            round ((sum(s.price_paid) / sum(s.qty_sold)), 2) as avg_ticket_price,
            isnull (sum(s.qty_sold), 0) as total_tickets_sold,
            isnull (sum(s.price_paid), 0) as total_sale_amount,
            isnull (count(e.event_id), 0) as total_events

        from sales as s
        right join events as e on s.event_id = e.event_id
        right join venues as v on e.venue_id = v.venue_id

        group by v.venue_id, v.venue_name, v.venue_city, v.venue_state, v.venue_seats

        order by v.venue_id 

    )

select *
from final
