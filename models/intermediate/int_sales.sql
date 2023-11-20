{{ config(materialized="view", bind=False) }}

with
    sales as (select * from {{ ref("stg_sales") }}),

    listings as (select * from {{ ref("stg_listings") }}),

    final as (

        select
            s.sale_id,
            s.list_id,
            s.seller_id,
            s.buyer_id,
            s.event_id,
            s.sale_date_id,
            l.list_date_id,
            s.qty_sold,
            s.ticket_price,
            s.price_paid,
            s.commission_prcnt,
            s.commission,
            s.earnings,
            s.sale_time,
            l.list_time,
            l.num_tickets as qty_listed

        from sales as s
        left join listings as l on s.list_id = l.list_id

        order by s.sale_id

    )

select * from final
