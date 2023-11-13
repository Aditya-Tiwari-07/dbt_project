{{ config(materialized="table", sort="event_id", dist="event_id") }}

with

    events as (select * from {{ ref("int_events") }}),

    final as (

        select
            e.event_id,
            e.event_name,
            e.start_time,
            e.min_ticket_price,
            e.max_ticket_price,
            e.avg_ticket_price,
            e.total_tickets_sold,
            e.total_sale_amount

        from events as e

        order by e.event_id

    )

select *
from final
