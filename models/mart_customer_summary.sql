{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_summary as (
    select
        -- dimensions
        customer_id,
        customer_name,
        segment,

        -- measures
        count(distinct order_id)                        as order_count,
        count(*)                                        as total_line_items,
        round(sum(sales), 2)                            as total_sales,
        round(sum(profit), 2)                           as total_profit,
        round(sum(profit) / nullif(sum(sales), 0), 4)  as profit_margin,
        min(order_date)                                 as first_order_date,
        max(order_date)                                 as last_order_date,
        date_diff(max(order_date), min(order_date), day) as customer_tenure_days

    from orders
    group by 1, 2, 3
)

select * from customer_summary