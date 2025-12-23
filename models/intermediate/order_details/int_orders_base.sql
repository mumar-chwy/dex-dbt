{{ config(
    materialized='ephemeral'
) }}

select
    s.order_id,
    s.shipment_tracking_number,
    s.order_placed_dttm_est,
    date(s.order_placed_dttm_est) as order_placed_date_est,
    s.release_dttm_est,
    s.warehouse_actual_ship_dttm_est,
    s.shipment_shipped_dttm_est,
    s.ffmcenter_id,
    s.postcode,
    s.carrier_code
from {{ ref('stg_chewybi__shipment_transactions') }} s
where date(s.order_placed_dttm_est) = '2025-12-19'
 )