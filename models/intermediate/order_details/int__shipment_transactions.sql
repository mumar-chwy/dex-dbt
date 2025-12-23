{{ config(materialized="table") }}

with
    shipment_aggregates as (

        select
            shipment_tracking_number,
            max(bulk_track_delivery_dttm) as max_bulk_track_delivery_dttm,
            max(shipment_tracking_initial_routing_location_city) as max_routing_city,
            max(shipment_tracking_initial_routing_location_state) as max_routing_state,
            max(shipment_tracking_initial_routing_dttm) as max_routing_dttm,
            max(initial_delivery_exception_dttm) as max_exception_dttm,
            max(initial_delivery_attempt_exception_cd) as max_exception_cd,
            max(trailer_number) as max_trailer_number,
            max(trailer_released_date) as max_trailer_released_date,
            max(trailer_created_date) as max_trailer_created_date,
            max(warehouse_actual_ship_dttm) as max_wh_ship_dttm,
            max(actual_ship_dttm) as max_actual_ship_dttm,
            max(actual_pulltime) as max_actual_pulltime,
            max(warehouse_carrier) as max_wh_carrier,
            max(warehouse_diverted_reason) as max_diverted_reason,
            max(warehouse_container_type) as max_container_type,
            max(actual_zone) as max_actual_zone,
            max(warehouse_actual_ship_route) as max_warehouse_actual_ship_route,
            max(actual_ship_route) as max_actual_ship_route,
            max(
                shipment_estimated_delivery_date
            ) as max_shipment_estimated_delivery_date,
            max(actual_ship_date) as max_actual_ship_date,
            max(order_placed_dttm_est) as max_order_placed_dttm_est,
            max(release_ffm_acknowldegement_dttm_est) as max_release_ffm_ack_dttm_est,
            max(release_dttm_est) as max_release_dttm_est,
            max(warehouse_planned_pull_dttm_est) as max_warehouse_planned_pull_dttm_est,
            max(warehouse_diverted_dttm_est) as max_warehouse_diverted_dttm_est,
            max(warehouse_actual_ship_dttm_est) as max_warehouse_actual_ship_dttm_est,
            max(warehouse_final_scan_dttm_est) as max_warehouse_final_scan_dttm_est,
            max(shipment_shipped_dttm_est) as max_shipment_shipped_dttm_est,
            max(release_payment_dttm_est) as max_release_payment_dttm_est,
            max(bulk_track_ship_dttm_est) as max_bulk_track_ship_dttm_est,
            max(
                bulk_track_estimated_delivery_dttm_est
            ) as max_bulk_track_estimated_delivery_dttm_est,
            max(bulk_track_last_status_dttm_est) as max_bulk_track_last_status_dttm_est,
            max(
                shipment_invoice_delivery_dttm_est
            ) as max_shipment_invoice_delivery_dttm_est,
            max(
                initial_delivery_attempt_dttm_est
            ) as max_initial_delivery_attempt_dttm_est
        from {{ ref("stg_chewybi__shipment_transactions") }}
        where shipment_estimated_delivery_date = '2025-12-20'
        group by shipment_tracking_number
    )

select
    st.order_id,
    st.shipment_tracking_number,
    st.address_id,
    st.ffmcenter_name,
    st.carrier_code,
    st.postcode,
    st.actual_transit_days,
    st.shipment_quantity,
    st.order_placed_dttm_est,

    /* Delivery & routing */
    coalesce(
        t.tnt_delivery_scan,
        st.bulk_track_delivery_dttm,
        sa.max_bulk_track_delivery_dttm
    ) as consolidated_bulk_track_delivery_dttm,

    coalesce(
        st.shipment_tracking_initial_routing_location_city, sa.max_routing_city
    ) as consolidated_shipment_tracking_initial_routing_location_city,

    coalesce(
        st.shipment_tracking_initial_routing_location_state, sa.max_routing_state
    ) as consolidated_shipment_tracking_initial_routing_location_state,

    coalesce(
        st.shipment_tracking_initial_routing_dttm, sa.max_routing_dttm
    ) as consolidated_shipment_tracking_initial_routing_dttm,

    /* Ship & warehouse lifecycle */
    coalesce(
        st.actual_ship_dttm, sa.max_actual_ship_dttm
    ) as consolidated_actual_ship_dttm,

    coalesce(
        st.actual_ship_date, sa.max_actual_ship_date
    ) as consolidated_actual_ship_date,

    coalesce(
        st.actual_pulltime, sa.max_actual_pulltime
    ) as consolidated_actual_pulltime,

    coalesce(
        st.warehouse_actual_ship_dttm, sa.max_wh_ship_dttm
    ) as consolidated_warehouse_actual_ship_dttm,

    coalesce(
        st.warehouse_planned_pull_dttm_est, sa.max_warehouse_planned_pull_dttm_est
    ) as consolidated_warehouse_planned_pull_dttm_est,

    coalesce(
        st.warehouse_diverted_dttm_est, sa.max_warehouse_diverted_dttm_est
    ) as consolidated_warehouse_diverted_dttm_est,

    coalesce(
        st.warehouse_actual_ship_dttm_est, sa.max_warehouse_actual_ship_dttm_est
    ) as consolidated_warehouse_actual_ship_dttm_est,

    coalesce(
        st.warehouse_final_scan_dttm_est, sa.max_warehouse_final_scan_dttm_est
    ) as consolidated_warehouse_final_scan_dttm_est,

    /* Routes, zones, containers */
    coalesce(st.actual_zone, sa.max_actual_zone) as consolidated_actual_zone,

    coalesce(
        st.actual_ship_route, sa.max_actual_ship_route
    ) as consolidated_actual_ship_route,

    coalesce(
        st.warehouse_actual_ship_route, sa.max_warehouse_actual_ship_route
    ) as consolidated_warehouse_actual_ship_route,

    coalesce(
        st.warehouse_container_type, sa.max_container_type
    ) as consolidated_warehouse_container_type,

    coalesce(st.warehouse_carrier, sa.max_wh_carrier) as consolidated_warehouse_carrier,

    /* Exceptions & trailers */
    coalesce(
        st.initial_delivery_exception_dttm, sa.max_exception_dttm
    ) as consolidated_initial_delivery_exception_dttm,

    coalesce(
        st.initial_delivery_attempt_exception_cd, sa.max_exception_cd
    ) as consolidated_initial_delivery_attempt_exception_cd,

    coalesce(st.trailer_number, sa.max_trailer_number) as consolidated_trailer_number,

    coalesce(
        st.trailer_released_date, sa.max_trailer_released_date
    ) as consolidated_trailer_released_date,

    coalesce(
        st.trailer_created_date, sa.max_trailer_created_date
    ) as consolidated_trailer_created_date,

    /* Bulk track lifecycle */
    coalesce(
        st.bulk_track_ship_dttm_est, sa.max_bulk_track_ship_dttm_est
    ) as consolidated_bulk_track_ship_dttm_est,

    coalesce(
        st.bulk_track_estimated_delivery_dttm_est,
        sa.max_bulk_track_estimated_delivery_dttm_est
    ) as consolidated_bulk_track_estimated_delivery_dttm_est,

    coalesce(
        st.bulk_track_last_status_dttm_est, sa.max_bulk_track_last_status_dttm_est
    ) as consolidated_bulk_track_last_status_dttm_est,

    /* Financial & invoice */
    coalesce(
        st.release_payment_dttm_est, sa.max_release_payment_dttm_est
    ) as consolidated_release_payment_dttm_est,

    coalesce(
        st.initial_delivery_attempt_dttm_est, sa.max_initial_delivery_attempt_dttm_est
    ) as consolidated_initial_delivery_attempt_dttm_est

from {{ ref("stg_chewybi__shipment_transactions") }} st
join shipment_aggregates sa on st.shipment_tracking_number = sa.shipment_tracking_number
left join
    {{ ref("stg_chewybi__carrier_tracking_events") }} t
    on st.shipment_tracking_number = t.tracking_number
where st.shipment_estimated_delivery_date = '2025-12-20'
