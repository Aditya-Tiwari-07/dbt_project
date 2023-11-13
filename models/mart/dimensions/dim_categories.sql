{{ config(materialized="table", sort="cat_id", dist="cat_id") }}

select * from {{ ref('int_categories') }}
