{{ config(materialized="table", sort="user_id", dist="user_id") }}

select * from {{ ref('stg_users') }}