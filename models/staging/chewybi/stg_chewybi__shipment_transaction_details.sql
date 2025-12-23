{{ 
    config(
        materialized='ephemeral'
    ) 
}}

with source as (
    select *
    from {{ source('chewybi', 'shipment_transaction_details') }}
    where ORDER_PLACED_DTTM >= current_date - 10
),

renamed as (
    select
        ORDER_LINE_ID,
        ORDER_ID,
        PRODUCT_ID,
        PRODUCT_PART_NUMBER,
        FFMCENTER_ID,
        FFMCENTER_NAME,
        SHIPMENT_PLANNED_WEIGHT,
        SHIPMENT_COST,
        SHIPMENT_SHIPPED_DTTM,
        SHIPMENT_TRACKING_NUMBER,
        SHIPMENT_QUANTITY,
        CARRIER_CODE,
        SHIPMODE_ID,
        ADDRESS_ID,
        RELEASE_STATUS,
        RELEASE_PAYMENT_DTTM,
        RELEASE_CUSTOMER_CONFIRM_DTTM,
        RELEASE_FFM_ACKNOWLDEGEMENT_DTTM,
        RELEASE_DTTM,
        ORDER_PLACED_DTTM,
        ORDER_AUTOSHIP_INTENTION,
        ORDER_SHIP_AS_COMPLETE,
        POSTCODE,
        PRIMARY_FFMCENTER_ID,
        PRIMARY_FFMCENTER_NAME,
        ACTUAL_ZONE,
        ACTUAL_TRANSIT_DAYS,
        ACTUAL_CUTOFF_TIME,
        DW_CREATE_DTTM,
        DW_UPDATE_DTTM,
        ACTUAL_SHIP_ROUTE,
        PRIMARY_SHIP_ROUTE,
        PRIMARY_TRANSIT_DAYS,
        ACTUAL_PULLTIME,
        PRIMARY_ZONE,
        ORDER_LINE_QUANTITY,
        ORDER_LINE_SHIPPED_DTTM,
        OPTIMAL_FFMCENTER_ID,
        OPTIMAL_FFMCENTER_NAME,
        OPTIMAL_SHIP_ROUTE,
        OPTIMAL_TRASNIT_DAYS,
        OPTIMAL_ZONE,
        PRODUCT_COMPANY_DESCRIPTION,
        CHEWY_PHARMACY_PART_NUMBER,
        SHIPMENT_COST_LOCAL
    from source
)

select *
from renamed
