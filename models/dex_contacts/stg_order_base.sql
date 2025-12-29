{{ 
  config(
    materialized='incremental',
    unique_key='order_id'
  ) 
}}

select
    order_placed_date,
    order_id,
    shipment_tracking_number,
    product_part_number,
    order_type,
    vet_diet,
    loctype,
    state,
    release_dttm,
    original_promised_pull_datetime,
    shipment_shipped_dttm,
    bulk_track_delivery_dttm,
    shipment_estimated_delivery_date,
    CTD,
    product_merch_classification1
from fulfillment_optimization_sandbox.dex_cx_base_dev

{% if is_incremental() %}
where order_placed_date >= (
    select max(order_placed_date) from {{ this }}
)
{% else %}
where order_placed_date >= (
    select min(order_date)
    from {{ ref('stg_contacts_base') }}
)
{% endif %}