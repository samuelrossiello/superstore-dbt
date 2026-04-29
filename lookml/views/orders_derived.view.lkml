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
    primary_key: yes
    hidden: yes
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
    label: "Customer ID"
    hidden: yes
  }

  dimension: customer_name {
    type: string
    sql: ${TABLE}.customer_name ;;
    label: "Customer name"
  }

  dimension: segment {
    type: string
    sql: ${TABLE}.segment ;;
    label: "Customer segment"
  }

  dimension: order_date {
    type: date
    sql: ${TABLE}.order_date ;;
    label: "Order date"
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
    label: "Region"
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
    label: "Category"
  }

  # ── Measures ─────────────────────────────────────────────

  measure: total_sales {
    type: sum
    sql: ${TABLE}.total_sales ;;
    label: "Total sales"
    value_format_name: usd_0
  }

  measure: total_profit {
    type: sum
    sql: ${TABLE}.total_profit ;;
    label: "Total profit"
    value_format_name: usd_0
  }

  measure: order_count {
    type: count_distinct
    sql: ${TABLE}.order_id ;;
    label: "Number of orders"
  }

  measure: weighted_profit_margin {
    type: number
    sql: ${total_profit} / NULLIF(${total_sales}, 0) ;;
    label: "Weighted profit margin"
    value_format_name: percent_2
  }

}