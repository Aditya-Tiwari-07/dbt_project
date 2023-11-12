{{ config(materialized='view', bind=False) }}

with source as (

    select * from {{ source('source_tickit', 'event') }}

),

renamed as (

    select
        eventid as event_id,
        venueid as venue_id,
        catid as cat_id,
        dateid as event_date_id,
        eventname as event_name,
        cast(starttime as time) as start_time
    from
        source
    where
        event_id IS NOT NULL
    order by
        event_id

)

select * from renamed