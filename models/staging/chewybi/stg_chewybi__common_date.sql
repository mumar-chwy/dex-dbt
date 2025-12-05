{{ 
    config(
        materialized='ephemeral'
    ) 
}}

with 
source as (
    select * from {{ source('chewybi', 'common_date') }}
),
renamed as (
    select
        common_date_key,
        common_date_dttm,
        common_date_text_long,
        common_date_text_short,
        common_year,
        common_quarter,
        common_month,
        common_day,
        common_month_name,
        common_quarter_name,
        common_day_name,
        common_last_day_of_month,
        common_holiday_season_name,
        common_holiday_season_flag,
        common_day_of_week,
        common_day_of_year,
        common_days_in_month,
        common_days_in_year,
        common_week_of_year,
        common_week_of_month,
        common_weekend_flag,
        common_holiday_flag,
        financial_calendar_reporting_month,
        financial_calendar_reporting_period,
        financial_calendar_reporting_quarter,
        financial_calendar_reporting_year,
        first_day_of_financial_calendar_reporting_period,
        last_day_of_financial_calendar_reporting_period,
        days_in_financial_calendar_reporting_period,
        first_day_of_financial_calendar_reporting_quarter,
        last_day_of_financial_calendar_reporting_quarter,
        days_in_financial_calendar_reporting_quarter,
        first_day_of_financial_calendar_reporting_year,
        last_day_of_financial_calendar_reporting_year,
        days_in_financial_calendar_reporting_year,
        financial_calendar_reporting_period_day_number,
        financial_calendar_reporting_period_days_left,
        chewy_financial_reporting_period,
        first_day_of_chewy_financial_reporting_period,
        last_day_of_chewy_financial_reporting_period,
        days_in_chewy_financial_reporting_period,
        chewy_financial_reporting_period_day_number,
        chewy_financial_reporting_period_days_left,
        financial_calendar_reporting_week,
        financial_calendar_first_day_reporting_week,
        financial_calendar_last_day_reporting_week,
        financial_calendar_reporting_week_of_year,
        peak_reporting_flag
    from source
)

select * from renamed
