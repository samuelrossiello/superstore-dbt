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

}