{{ config(materialized="view", bind=False) }}

with
    sales as (select * from {{ ref("stg_sales") }}),

    events as (select * from {{ ref("stg_events") }}),

    final as (

        select
            e.event_id,
            e.venue_id,
            e.cat_id,
            e.event_date_id,
            e.event_name,
            e.start_time,
            min(s.ticket_price) as min_ticket_price,
            max(s.ticket_price) as max_ticket_price,
            round((sum(s.price_paid) / sum(s.qty_sold)), 2) as avg_ticket_price,
            isnull (sum(s.qty_sold), 0) as total_tickets_sold,
            isnull (sum(s.price_paid), 0) as total_sale_amount

        from sales as s
        right join events as e on s.event_id = e.event_id

        group by e.event_id, e.venue_id, e.cat_id, e.event_date_id, e.event_name, e.start_time

        order by e.event_id

    )

select * from final
