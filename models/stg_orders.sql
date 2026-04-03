with source as (
    select * from {{ source('superstore', 'orders') }}
),

renamed as (
    select
        -- identifiers
        row_id,
        order_id,
        customer_id,
        product_id,

        -- dates
        order_date,
        ship_date,

        -- dimensions
        ship_mode,
        customer_name,
        segment,
        country_region,
        city,
        state_province,
        postal_code,
        region,
        category,
        sub_category,
        product_name,

        -- measures
        round(sales, 2) as sales,
        quantity,
        round(discount, 4) as discount,
        round(profit, 2) as profit,

        -- derived
        round(profit / nullif(sales, 0), 4) as profit_margin,
        date_diff(ship_date, order_date, day) as days_to_ship

    from source
)

select * from renamed