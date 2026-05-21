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
    group_label: "Identity"
    description: "Full name of the customer"
  }

  dimension: segment {
    type: string
    sql: ${TABLE}.segment ;;
    label: "Customer segment"
    group_label: "Segment"
    description: "Customer segment - Consumer, Corporate, or Home Office"
  }

  dimension: preferred_category {
    type: string
    sql: ${TABLE}.preferred_category ;;
    label: "Preferred category"
    group_label: "Segment"
    description: "The product category the customer orders from most frequently by sales volume"
  }

  dimension: is_high_value_customer {
    type: yesno
    sql: ${TABLE}.lifetime_sales > 5000 ;;
    label: "Is high value customer?"
    group_label: "Behavior"
    description: "Yes if customer lifetime sales exceed $5,000"
  }

  dimension: order_count_tier {
    type: tier
    tiers: [1, 3, 6, 10, 20]
    style: interval
    sql: ${TABLE}.order_count ;;
    label: "Order count tier"
    group_label: "Behavior"
    description: "Customer grouped by number of orders placed"
  }

  dimension: customer_acquisition_cohort {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.first_order_date < '2021-01-01' THEN 'Pre-2021'
        WHEN ${TABLE}.first_order_date >= '2021-01-01' AND ${TABLE}.first_order_date < '2022-01-01' THEN '2021'
        WHEN ${TABLE}.first_order_date >= '2022-01-01' AND ${TABLE}.first_order_date < '2023-01-01' THEN '2022'
        WHEN ${TABLE}.first_order_date >= '2023-01-01' AND ${TABLE}.first_order_date < '2024-01-01' THEN '2023'
        ELSE '2024+'
      END
    ;;
    label: "Customer acquisition cohort"
    group_label: "Cohort"
    description: "Groups customers into yearly cohorts based on their first order date — true acquisition cohort for tracking customer behavior over time"
  }

  # ── Measures ─────────────────────────────────────────────

  measure: total_customers {
    type: count_distinct
    sql: ${TABLE}.customer_id ;;
    label: "Total customers"
    group_label: "Customers"
    description: "Count of distinct customers"
  }

  measure: avg_order_value {
    type: average
    sql: ${TABLE}.avg_order_value ;;
    label: "Avg order value"
    value_format_name: usd_0
    group_label: "Sales"
    description: "Average order value per customer"
  }

  measure: avg_days_between_orders {
    type: average
    sql: ${TABLE}.avg_days_between_orders ;;
    label: "Avg days between orders"
    value_format_name: decimal_1
    group_label: "Behavior"
    description: "Average number of days between a customer's orders"
  }

  measure: avg_customer_tenure {
    type: average
    sql: ${TABLE}.customer_tenure_days ;;
    label: "Avg customer tenure (days)"
    value_format_name: decimal_0
    group_label: "Behavior"
    description: "Average number of days between first and last order across customers"
  }

  measure: total_lifetime_sales {
    type: sum
    sql: ${TABLE}.lifetime_sales ;;
    label: "Total lifetime sales"
    value_format_name: usd_0
    group_label: "Sales"
    description: "Sum of lifetime sales across all customers in the result set"
  }

}