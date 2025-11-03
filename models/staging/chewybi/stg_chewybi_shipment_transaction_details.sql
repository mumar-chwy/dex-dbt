with source as 
(
    select * from {{ source('chewybi','shipment_transaction_details') }}
)

select distinct
    shipment_tracking_number
  from 
    source
where product_merch_Classification1 ='Specialty' and product_merch_Classification4 ilike '%Live%'