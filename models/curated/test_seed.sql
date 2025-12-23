{{ config(
    materialized = 'incremental',
    unique_key = ['shipment_tracking_number'],
    incremental_strategy = 'delete+insert'
) }}

with date_bounds as (
    select
        date_trunc('day', current_date) - interval '2 day' as start_date,
        date_trunc('day', current_date) - interval '1 day'  as end_date
),

/* ---------------- Autoship ---------------- */

as_type as (
    select distinct
        orders_id as order_id,
        case
            when value = 'ORDER_NOW' then 'Auto Ship Ship Now'
            when value = 'PARENT' then 'Auto Ship First Time'
            when value = 'SYSTEM_GENERATED' then 'Auto Ship System'
        end as as_type
    from {{ source('order_management_service', 'orderattr') }}
    where name = 'AUTOSHIP_TYPE'
      and timecreated >= date_trunc('week', current_date) - interval '1 week'
),

/* ---------------- FC Times ---------------- */

fc_time as (
    select distinct
        b.tracking_number::varchar(25) as tracking_number,
        convert_timezone('UTC','America/New_York', b.arrive_date) as fc_enter_time_est,
        convert_timezone('UTC','America/New_York', a.promised_cut_datetime) as promised_cut_datetime_est,
        convert_timezone('UTC','America/New_York', d.original_promised_pull_datetime) as promised_pull_datetime_est,
        convert_timezone('UTC','America/New_York', b.actual_ship_date) as actual_ship_date_est,
        b.wh_id,
        c.nostra_sbd_flag
    from {{ source('aad', 'v_pick_container_cutoff') }} a
    join {{ source('aad', 't_pick_container') }} b
        on a.container_id = b.container_id
    join {{ source('aad', 't_order') }} c
        on c.order_number = b.order_number
    join {{ source('aad', 'v_pick_container_dates') }} d
        on d.container_id = a.container_id
    where date(b.arrive_date) >= date_trunc('week', current_date) - interval '1 week'
)
/* ================= FINAL SELECT ================= */

select
    cd.common_date_dttm,
    s.order_id,
    coalesce(a.as_type, 'Non Auto Ship') as order_type,
    s.shipment_tracking_number,
    s.order_placed_dttm_est,
    date(s.order_placed_dttm_est) as order_placed_date_est,
    s.release_dttm_est,
    fc.fc_enter_time_est,
    s.warehouse_actual_ship_dttm_est,
    fc.promised_pull_datetime_est,
    coalesce(s.bulk_track_delivery_dttm_est, ds.tnt_delivery_scan) as bulk_track_delivery_dttm_est
from {{ source('chewybi', 'shipment_transactions') }} s
join {{ source('chewybi', 'common_date') }} cd
    on cd.common_date_dttm = date(s.order_placed_dttm_est)
left join {{ ref ("stg_chewybi__carrier_tracking_events")}} ds
    on s.shipment_tracking_number = ds.tracking_number
left join as_type a
    on a.order_id = s.order_id
left join fc_time fc
    on fc.tracking_number = s.shipment_tracking_number
where date(s.order_placed_dttm_est)
      between (select start_date from date_bounds)
          and (select end_date from date_bounds)

{% if is_incremental() %}
  and date(s.order_placed_dttm_est) >= (select min(start_date) from date_bounds)
{% endif %}
