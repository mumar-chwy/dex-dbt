with source as (select * from {{ source("chewybi", "products") }})

select distinct product_part_number, product_name
from source
where
    product_merch_classification1 = 'Specialty'
    and product_merch_classification4 ilike '%Live%'
    limit 100
