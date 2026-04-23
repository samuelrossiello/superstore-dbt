view: mart_sales_summary {
  sql_table_name: `gothic-sum-492014-r0.dbt_srossiello.mart_sales_summary` ;;

  label: "Sales Summary"

  # ── Dimensions ──────────────────────────────────────────

  dimension: order_date {
    type: date
    sql: ${TABLE}.order_date ;;
    label: "Order date"
    description: "The date the order was placed"
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

  dimension: segment {
    type: string
    sql: ${TABLE}.segment ;;
    label: "Customer segment"
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
    label: "State"
  }

  dimension: is_profitable {
    type: yesno
    sql: ${TABLE}.total_profit > 0 ;;
    label: "Is profitable?"
    description: "Yes if the order generated positive profit"
  }

  dimension: sales_tier {
    type: tier
    tiers: [0, 100, 500, 1000, 5000]
    style: interval
    sql: ${TABLE}.total_sales ;;
    label: "Sales tier"
    description: "Sales grouped into performance bands"
  }

  # ── Measures ─────────────────────────────────────────────

  measure: total_sales {
    type: sum
    sql: ${TABLE}.total_sales ;;
    label: "Total sales"
    value_format_name: usd_0
    description: "Sum of sales across all orders in the result set"
  }

  measure: order_count {
    type: count_distinct
    sql: ${TABLE}.order_id ;;
    label: "Number of orders"
    description: "Count of distinct orders"
  }

  measure: total_profit {
    type: sum
    sql: ${TABLE}.total_profit ;;
    label: "Total profit"
    value_format_name: usd_0
  }

  measure: total_quantity {
    type: sum
    sql: ${TABLE}.total_quantity ;;
    label: "Total quantity"
    value_format_name: decimal_0
    description: "Sum of all items sold"
  }

  measure: avg_discount {
    type: average
    sql: ${TABLE}.avg_discount ;;
    label: "Avg discount"
    value_format_name: percent_2
    description: "Average discount rate across orders"
  }

  measure: max_sales {
    type: max
    sql: ${TABLE}.total_sales ;;
    label: "Max order sales"
    value_format_name: usd_0
    description: "Highest single order sales value in the result set"
  }

  measure: min_sales {
    type: min
    sql: ${TABLE}.total_sales ;;
    label: "Min order sales"
    value_format_name: usd_0
    description: "Lowest single order sales value in the result set"
  }

  measure: furniture_sales {
    type: sum
    sql: ${TABLE}.total_sales ;;
    filters: [category: "Furniture"]
    label: "Furniture sales"
    value_format_name: usd_0
    description: "Total sales for Furniture category only"
  }

  measure: total_sales_broken {
    type: sum
    sql: ${TABLE}.total_sales ;;
    label: "Total sales (broken - fanout risk)"
    value_format_name: usd_0
    description: "This measure would double-count in a fanout join scenario"
  }

  measure: total_sales_safe {
    type: sum_distinct
    sql_distinct_key: ${TABLE}.order_id ;;
    sql: ${TABLE}.total_sales ;;
    label: "Total sales (fanout safe)"
    value_format_name: usd_0
    description: "Uses sum_distinct to safely aggregate across joins"
  }
  
}