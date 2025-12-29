{{ config(materialized='table') }}

select
    common_date_dttm::date as date_day
from {{ source('chewybi','common_date')}}
where common_date_dttm::date >= current_date - interval '3 weeks'
  and common_date_dttm::date < current_date