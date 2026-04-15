{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_cohorts as (
    select
        customer_id,
        customer_name,
        segment,
        date_trunc(min(order_date), month) as cohort_month
    from orders
    group by 1, 2, 3
),

order_periods as (
    select
        o.customer_id,
        o.order_id,
        o.order_date,
        o.sales,
        o.profit,
        c.cohort_month,
        c.segment,
        date_diff(
            date_trunc(o.order_date, month),
            c.cohort_month,
            month
        ) as period_number
    from orders o
    left join customer_cohorts c
        on o.customer_id = c.customer_id
),

cohort_metrics as (
    select
        cohort_month,
        period_number,
        segment,
        count(distinct customer_id)     as customer_count,
        count(distinct order_id)        as order_count,
        round(sum(sales), 2)            as total_sales,
        round(sum(profit), 2)           as total_profit,
        round(avg(sales), 2)            as avg_sales_per_order,
        round(sum(sales) / nullif(count(distinct customer_id), 0), 2) as avg_sales_per_customer

    from order_periods
    group by 1, 2, 3
)

select * from cohort_metrics
order by cohort_month, period_number