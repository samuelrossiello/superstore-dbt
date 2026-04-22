view: mart_customer_summary {
  sql_table_name: `gothic-sum-492014-r0.dbt_srossiello.mart_customer_summary` ;;

  label: "Customer Summary"

  # ── Dimensions ──────────────────────────────────────────

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
    label: "Customer ID"
    primary_key: yes
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

  dimension: preferred_category {
    type: string
    sql: ${TABLE}.preferred_category ;;
    label: "Preferred category"
  }

  # ── Measures ─────────────────────────────────────────────

  measure: total_customers {
    type: count_distinct
    sql: ${TABLE}.customer_id ;;
    label: "Total customers"
    description: "Count of distinct customers"
  }

  measure: avg_order_value {
    type: average
    sql: ${TABLE}.avg_order_value ;;
    label: "Avg order value"
    value_format_name: usd_0
    description: "Average order value per customer"
  }

  measure: avg_days_between_orders {
    type: average
    sql: ${TABLE}.avg_days_between_orders ;;
    label: "Avg days between orders"
    value_format_name: decimal_1
    description: "Average number of days between a customer's orders"
  }

}