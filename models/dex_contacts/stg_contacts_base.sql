{{ config(materialized='table') }}

select
    c.leg_start_dttm,
    c.leg_id,
    o.order_placed_dttm_est::date as order_date,
    c.order_id,
    c.queue_display_name,
    c.category_level_1,
    c.category_level_2,
    c.category_level_3,
    c.category_level_4,
    s.pcr_level_1,
    s.pcr_level_2,
    s.pcr_level_3
from cs_sandbox.vw_contacts_brd_est c
left join bt_customer_care.fct_conversation_summaries s
  on c.session_id = s.conversation_id
left join chewybi.orders o
  on c.order_id = o.order_id
where c.category_primary_owner = 'DEX'
  and cs_contact_rate_num = 1
  and c.leg_start_dttm::date in (
        select date_day from {{ ref('stg_dates') }}
      )
  and o.order_placed_dttm_est::date >= '2025-01-01'
  and o.order_status <> 'X'
