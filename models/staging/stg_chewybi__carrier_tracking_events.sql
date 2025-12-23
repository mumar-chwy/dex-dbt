{{ config(
    materialized='table'  
) }}

WITH ranked_scans AS (
    SELECT
        carrier,
        tracking_number,
        event_status,
        status_code,
        event_dttm AS tnt_delivery_scan,
        ROW_NUMBER() OVER (
            PARTITION BY carrier, tracking_number
            ORDER BY event_dttm DESC
        ) AS rn
    FROM {{ source('bt_sc_transportation', 'carrier_tracking_events') }}
    WHERE event_status = 'DELIVERED'
      AND event_dttm::date = '2025-12-19'
)

SELECT
    carrier,
    tracking_number,
    event_status,
    status_code,
    tnt_delivery_scan
FROM ranked_scans
WHERE rn = 1