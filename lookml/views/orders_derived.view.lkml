view: orders_derived {
  derived_table: {
    sql:
      SELECT
        order_id,
        customer_id,
        customer_name,
        segment,
        order_date,
        region,
        state_province,
        category,
        sub_category,
        SUM(sales)    AS total_sales,
        SUM(profit)   AS total_profit,
        SUM(quantity) AS total_quantity
      FROM `gothic-sum-492014-r0.dbt_srossiello.stg_orders`
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ;;
  }

  label: "Orders"

  # ── Dimensions ──────────────────────────────────────────

  dimension: order_id {
    type: string
    sql: ${TABLE}.order_id ;;
    label: "Order ID"
    group_label: "Identity"
    primary_key: yes
    hidden: yes
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
    label: "Customer ID"
    group_label: "Identity"
    hidden: yes
  }

  dimension: customer_name {
    type: string
    sql: ${TABLE}.customer_name ;;
    label: "Customer name"
    group_label: "Identity"
    description: "Full name of the customer who placed the order"
  }

  dimension: segment {
    type: string
    sql: ${TABLE}.segment ;;
    label: "Customer segment"
    group_label: "Customer"
    description: "Customer segment - Consumer, Corporate, or Home Office"
  }

  dimension: order_date {
    type: date
    sql: ${TABLE}.order_date ;;
    label: "Order date"
    group_label: "Time"
    description: "The date the order was placed"
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
    label: "Region"
    group_label: "Location"
    description: "Geographic sales region - West, East, Central, or South"
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
    label: "Category"
    group_label: "Product"
    description: "Product category - Furniture, Technology, or Office Supplies"
  }

  # ── Measures ─────────────────────────────────────────────

  measure: total_sales {
    type: sum
    sql: ${TABLE}.total_sales ;;
    label: "Total sales"
    group_label: "Sales"
    value_format_name: usd_0
    description: "Sum of sales across all orders in the result set"
  }

  measure: total_profit {
    type: sum
    sql: ${TABLE}.total_profit ;;
    label: "Total profit"
    group_label: "Profit"
    value_format_name: usd_0
    description: "Sum of profit across all orders in the result set"
  }

  measure: order_count {
    type: count_distinct
    sql: ${TABLE}.order_id ;;
    label: "Number of orders"
    group_label: "Volume"
    description: "Count of distinct orders"
  }

  measure: weighted_profit_margin {
    type: number
    sql: ${total_profit} / NULLIF(${total_sales}, 0) ;;
    label: "Weighted profit margin"
    group_label: "Sales"
    value_format_name: percent_2
    description: "True weighted profit margin: total profit divided by total sales"
  }

}