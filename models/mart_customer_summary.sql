{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_metrics as (
    select
        -- dimensions
        customer_id,
        customer_name,
        segment,

        -- measures
        count(distinct order_id)                                    as order_count,
        count(*)                                                    as total_line_items,
        round(sum(sales), 2)                                        as lifetime_sales,
        round(sum(profit), 2)                                       as lifetime_profit,
        round(sum(profit) / nullif(sum(sales), 0), 4)              as lifetime_profit_margin,
        min(order_date)                                             as first_order_date,
        max(order_date)                                             as last_order_date,
        date_diff(max(order_date), min(order_date), day)           as customer_tenure_days,
        round(sum(sales) / nullif(count(distinct order_id), 0), 2) as avg_order_value,
        round(date_diff(max(order_date), min(order_date), day)
              / nullif(count(distinct order_id) - 1, 0), 1)        as avg_days_between_orders

    from orders
    group by 1, 2, 3
),

category_sales as (
    select
        customer_id,
        category,
        sum(sales) as category_sales,
        row_number() over (
            partition by customer_id
            order by sum(sales) desc
        ) as category_rank
    from orders
    group by 1, 2
),

preferred_category as (
    select
        customer_id,
        category as preferred_category
    from category_sales
    where category_rank = 1
)

select
    m.*,
    p.preferred_category
from customer_metrics m
left join preferred_category p
    on m.customer_id = p.customer_id