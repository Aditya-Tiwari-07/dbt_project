{{ config(materialized="table", sort="venue_id", dist="venue_id") }}

select * from {{ ref('int_venues') }}