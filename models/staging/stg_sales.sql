{{ config(materialized='view', bind=False) }}

with source as (

    select * from {{ source('source_tickit', 'sales') }}

),

renamed as (

    select
        salesid as sale_id,
        listid as list_id,
        sellerid as seller_id,
        buyerid as buyer_id,
        eventid as event_id,
        dateid as sale_date_id,
        qtysold as qty_sold,
        round (pricepaid / qtysold, 2) as ticket_price,
        pricepaid as price_paid,
        round((commission / pricepaid) * 100, 2) as commission_prcnt,
        commission,
        (pricepaid - commission) as earnings,
        cast(saletime as time) as sale_time
    from
        source
    where
        sale_id IS NOT NULL
    order by
        sale_id

)

select * from renamed
