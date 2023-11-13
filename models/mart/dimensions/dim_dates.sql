{{ config(materialized="table", sort="date_id", dist="date_id") }}

select * from {{ ref('stg_dates') }}
