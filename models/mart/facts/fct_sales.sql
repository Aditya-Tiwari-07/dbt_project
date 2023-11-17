{{ config(materialized="table", sort="sale_id", dist="sale_id") }}

with

    sales as (select * from {{ ref("int_sales") }}),

    events as (select * from {{ ref("int_events") }}),

    categories as (select * from {{ ref("int_categories") }}),

    venues as (select * from {{ ref("int_venues") }}),

    final as (

        select
            s.sale_id,
            s.list_id,
            s.seller_id,
            s.buyer_id,
            s.event_id,
            c.cat_id,
            v.venue_id,
            s.sale_date_id,
            s.list_date_id,
            e.event_date_id,
            s.qty_sold,
            s.ticket_price,
            s.price_paid,
            s.commission_prcnt,
            s.commission,
            s.earnings,
            s.sale_time,
            s.list_time,
            e.start_time,
            s.qty_listed

        from sales as s
        left join events as e on s.event_id = e.event_id
        left join venues as v on e.venue_id = v.venue_id
        left join categories as c on e.cat_id = c.cat_id

        order by s.sale_id

    )

select *
from final
