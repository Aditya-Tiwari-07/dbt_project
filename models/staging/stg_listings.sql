{{ config(materialized='view', bind=False) }}

with source as (

    select * from {{ source('source_tickit', 'listing') }}

),

renamed as (

    select
        listid as list_id,
        sellerid as seller_id,
        eventid as event_id,
        dateid as list_date_id,
        numtickets as num_tickets,
        priceperticket as price_per_ticket,
        totalprice as total_price,
        cast(listtime as time) as list_time

    from
        source
    where
        list_id IS NOT NULL
    order by
        list_id

)

select *  from renamed
