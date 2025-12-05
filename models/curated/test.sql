{{ config(materialized="incremental", incremental_strategy="merge") }}

select *
from {{ ref("stg_chewybi__products") }}
