- dashboard: customer_analytics
  title: Superstore Customer Analytics
  layout: newspaper
  preferred_viewer: dashboards-next

  filters:
  - name: date_filter
    title: Date Range
    type: date_filter
    default_value: "90 days"
    allow_multiple_values: false
    required: false

  - name: segment_filter
    title: Segment
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    model: superstore
    explore: customer_summary
    field: mart_customer_summary.segment

  - name: preferred_category_filter
    title: Preferred Category
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    model: superstore
    explore: customer_summary
    field: mart_customer_summary.preferred_category

  elements:
  - title: Total Customers
    name: total_customers_scorecard
    model: superstore
    explore: customer_summary
    type: single_value
    fields: [mart_customer_summary.total_customers]
    listen:
      segment_filter: mart_customer_summary.segment
      preferred_category_filter: mart_customer_summary.preferred_category
    row: 0
    col: 0
    width: 6
    height: 4

  - title: Avg Order Value
    name: avg_order_value_scorecard
    model: superstore
    explore: customer_summary
    type: single_value
    fields: [mart_customer_summary.avg_order_value]
    listen:
      segment_filter: mart_customer_summary.segment
      preferred_category_filter: mart_customer_summary.preferred_category
    row: 0
    col: 6
    width: 6
    height: 4

  - title: Avg Days Between Orders
    name: avg_days_scorecard
    model: superstore
    explore: customer_summary
    type: single_value
    fields: [mart_customer_summary.avg_days_between_orders]
    listen:
      segment_filter: mart_customer_summary.segment
      preferred_category_filter: mart_customer_summary.preferred_category
    row: 0
    col: 12
    width: 6
    height: 4

  - title: Total Lifetime Sales
    name: total_lifetime_sales_scorecard
    model: superstore
    explore: customer_summary
    type: single_value
    fields: [mart_customer_summary.total_lifetime_sales]
    listen:
      segment_filter: mart_customer_summary.segment
      preferred_category_filter: mart_customer_summary.preferred_category
    row: 0
    col: 18
    width: 6
    height: 4

  - title: Avg Order Value by Segment
    name: avg_order_value_by_segment
    model: superstore
    explore: customer_summary
    type: looker_bar
    fields: [mart_customer_summary.segment, mart_customer_summary.avg_order_value]
    sorts: [mart_customer_summary.avg_order_value desc]
    listen:
      segment_filter: mart_customer_summary.segment
      preferred_category_filter: mart_customer_summary.preferred_category
    row: 4
    col: 0
    width: 24
    height: 8

  - title: Customers by Preferred Category
    name: customers_by_preferred_category
    model: superstore
    explore: customer_summary
    type: looker_bar
    fields: [mart_customer_summary.preferred_category, mart_customer_summary.total_customers]
    sorts: [mart_customer_summary.total_customers desc]
    listen:
      segment_filter: mart_customer_summary.segment
      preferred_category_filter: mart_customer_summary.preferred_category
    row: 12
    col: 0
    width: 12
    height: 8

  - title: Top 10 Customers by Lifetime Sales
    name: top_customers_table
    model: superstore
    explore: customer_summary
    type: looker_grid
    fields: [mart_customer_summary.customer_name, mart_customer_summary.total_lifetime_sales, mart_customer_summary.avg_order_value]
    sorts: [mart_customer_summary.total_lifetime_sales desc]
    limit: 10
    listen:
      segment_filter: mart_customer_summary.segment
      preferred_category_filter: mart_customer_summary.preferred_category
    row: 12
    col: 12
    width: 12
    height: 8