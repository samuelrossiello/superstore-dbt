{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

summary as (
    select
        -- dimensions
        order_date,
        region,
        state_province,
        category,
        sub_category,
        segment,

        -- measures
        round(sum(sales), 2)                           as total_sales,
        round(sum(profit), 2)                          as total_profit,
        round(sum(profit) / nullif(sum(sales), 0), 4) as profit_margin,
        sum(quantity)                                  as total_quantity,
        round(avg(discount), 4)                        as avg_discount

    from orders
    group by 1, 2, 3, 4, 5, 6
)

select * from summary