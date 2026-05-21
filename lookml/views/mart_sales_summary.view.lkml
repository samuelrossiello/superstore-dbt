view: mart_sales_summary {
  sql_table_name: `gothic-sum-492014-r0.dbt_srossiello.mart_sales_summary` ;;

  label: "Sales Summary"

  # ── Dimensions ──────────────────────────────────────────

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
    description: "Geographic sales region — West, East, Central, or South"
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
    label: "Category"
    group_label: "Product"
    description: "Product category — Furniture, Technology, or Office Supplies"
  }

  dimension: sub_category {
    type: string
    sql: ${TABLE}.sub_category ;;
    label: "Sub-category"
    description: "Product sub-category within the main category"
    group_label: "Product"
  }

  dimension: segment {
    type: string
    sql: ${TABLE}.segment ;;
    label: "Customer segment"
    group_label: "Customer"
    description: "Customer segment — Consumer, Corporate, or Home Office"
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
    label: "State"
    group_label: "Location"
    description: "US state where the order was shipped"
  }

  dimension: is_profitable {
    type: yesno
    sql: ${TABLE}.total_profit > 0 ;;
    label: "Is profitable?"
    group_label: "Performance"
    description: "Yes if the order generated positive profit"
  }

  dimension: sales_tier {
    type: tier
    tiers: [0, 100, 500, 1000, 5000]
    style: interval
    sql: ${TABLE}.total_sales ;;
    label: "Sales tier"
    group_label: "Performance"
    description: "Sales grouped into performance bands"
  }

  parameter: group_by_selector {
    type: unquoted
    label: "Group by"
    group_label: "Dynamic"
    description: "Select the dimension to group by in the explore"
    allowed_value: {
      label: "Region"
      value: "region"
    }
    allowed_value: {
      label: "Category"
      value: "category"
    }
    allowed_value: {
      label: "Segment"
      value: "segment"
    }
    allowed_value: {
      label: "State"
      value: "state_province"
    }
    default_value: "region"
  }

  dimension: dynamic_group_by {
    type: string
    label: "Dynamic group by"
    group_label: "Dynamic"
    sql:
      {% if group_by_selector._parameter_value == 'region' %}
        ${TABLE}.region
      {% elsif group_by_selector._parameter_value == 'category' %}
        ${TABLE}.category
      {% elsif group_by_selector._parameter_value == 'segment' %}
        ${TABLE}.segment
      {% elsif group_by_selector._parameter_value == 'state_province' %}
        ${TABLE}.state_province
      {% else %}
        ${TABLE}.region
      {% endif %}
    ;;
    description: "Dynamic dimension that changes based on the group by selector parameter"
  }

  parameter: date_granularity {
    type: unquoted
    label: "Date granularity"
    group_label: "Time"
    description: "Select the date granularity for the time dimension"
    allowed_value: {
      label: "Day"
      value: "DATE"
    }
    allowed_value: {
      label: "Month"
      value: "DATE_TRUNC"
    }
    allowed_value: {
      label: "Quarter"
      value: "DATE_TRUNC_QUARTER"
    }
    allowed_value: {
      label: "Year"
      value: "DATE_TRUNC_YEAR"
    }
    default_value: "DATE_TRUNC"
  }

  dimension: dynamic_date {
    type: string
    label: "Dynamic date"
    group_label: "Time"
    sql:
      {% if date_granularity._parameter_value == 'DATE' %}
        CAST(${TABLE}.order_date AS STRING)
      {% elsif date_granularity._parameter_value == 'DATE_TRUNC' %}
        CAST(DATE_TRUNC(${TABLE}.order_date, MONTH) AS STRING)
      {% elsif date_granularity._parameter_value == 'DATE_TRUNC_QUARTER' %}
        CAST(DATE_TRUNC(${TABLE}.order_date, QUARTER) AS STRING)
      {% elsif date_granularity._parameter_value == 'DATE_TRUNC_YEAR' %}
        CAST(DATE_TRUNC(${TABLE}.order_date, YEAR) AS STRING)
      {% else %}
        CAST(DATE_TRUNC(${TABLE}.order_date, MONTH) AS STRING)
      {% endif %}
    ;;
    description: "Dynamic date dimension that changes granularity based on the date granularity parameter"
  }

  filter: category_filter {
    type: string
    label: "Category filter"
    description: "Filter total sales to a specific category"
    suggest_dimension: category
    group_label: "Product"
  }

  dimension: customer_order_cohort {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.order_date >= '2023-01-01' AND ${TABLE}.order_date < '2024-01-01' THEN '2023'
        WHEN ${TABLE}.order_date >= '2024-01-01' AND ${TABLE}.order_date < '2025-01-01' THEN '2024'
        WHEN ${TABLE}.order_date >= '2025-01-01' AND ${TABLE}.order_date < '2026-01-01' THEN '2025'
        ELSE '2026+'
      END
    ;;
    label: "Order cohort"
    group_label: "Performance"
    description: "Groups orders into yearly cohorts based on order date. Covers 2023 through 2026+ based on actual dataset range."
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

  measure: order_count {
    type: count_distinct
    sql: ${TABLE}.order_id ;;
    label: "Number of orders"
    group_label: "Volume"
    description: "Count of distinct orders"
  }

  measure: total_profit {
    type: sum
    sql: ${TABLE}.total_profit ;;
    label: "Total profit"
    group_label: "Profit"
    value_format_name: usd_0
    description: "Sum of profit across all orders in the result set"
  }

  measure: total_quantity {
    type: sum
    sql: ${TABLE}.total_quantity ;;
    label: "Total quantity"
    group_label: "Volume"
    value_format_name: decimal_0
    description: "Sum of all items sold"
  }

  measure: avg_discount {
    type: average
    sql: ${TABLE}.avg_discount ;;
    label: "Avg discount"
    group_label: "Discounting"
    value_format_name: percent_2
    description: "Average discount rate across orders"
  }

  measure: max_sales {
    type: max
    sql: ${TABLE}.total_sales ;;
    label: "Max order sales"
    group_label: "Ranges"
    value_format_name: usd_0
    description: "Highest single order sales value in the result set"
  }

  measure: min_sales {
    type: min
    sql: ${TABLE}.total_sales ;;
    label: "Min order sales"
    group_label: "Ranges"
    value_format_name: usd_0
    description: "Lowest single order sales value in the result set"
  }

  measure: furniture_sales {
    type: sum
    sql: ${TABLE}.total_sales ;;
    filters: [category: "Furniture"]
    label: "Furniture sales"
    group_label: "Sales"
    value_format_name: usd_0
    description: "Total sales for Furniture category only"
  }

  measure: total_sales_broken {
    type: sum
    sql: ${TABLE}.total_sales ;;
    label: "Total sales (broken - fanout risk)"
    group_label: "Sales"
    value_format_name: usd_0
    description: "This measure would double-count in a fanout join scenario"
    hidden: yes
  }

  measure: total_sales_safe {
    type: sum_distinct
    sql_distinct_key: ${TABLE}.order_id ;;
    sql: ${TABLE}.total_sales ;;
    label: "Total sales (fanout safe)"
    group_label: "Sales"
    value_format_name: usd_0
    description: "Uses sum_distinct to safely aggregate across joins"
    hidden: yes
  }
  
  measure: weighted_profit_margin {
    type: number
    sql: ${total_profit} / NULLIF(${total_sales}, 0) ;;
    label: "Weighted profit margin"
    group_label: "Profit"
    value_format_name: percent_2
    description: "True weighted profit margin: total profit divided by total sales. Avoids average of averages by recalculating dynamically at query time."
  }

  measure: total_sales_prior_period {
    type: sum
    sql: ${TABLE}.total_sales ;;
    label: "Total sales (prior period)"
    group_label: "Sales"
    value_format_name: usd_0
    description: "Total sales for the prior period — used for period-over-period comparison"
    filters: [order_date: "last year"]
  }

  measure: sales_period_over_period_change {
    type: number
    sql: (${total_sales} - ${total_sales_prior_period}) / NULLIF(${total_sales_prior_period}, 0) ;;
    label: "Sales PoP change"
    group_label: "Sales"
    value_format_name: percent_2
    description: "Percentage change in total sales vs prior period"
  }

  measure: sales_running_total {
    type: running_total
    sql: ${total_sales} ;;
    label: "Sales running total"
    group_label: "Sales"
    value_format_name: usd_0
    description: "Cumulative sales running total — only meaningful when sorted by date"
  }

  measure: filtered_category_sales {
    type: sum
    sql: ${TABLE}.total_sales ;;
    label: "Filtered category sales"
    group_label: "Sales"
    value_format_name: usd_0
    description: "Total sales filtered to the category selected in the category filter"
    filters: [category_filter: "{% parameter category_filter %}"]
  }

}