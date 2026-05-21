- dashboard: orders
  title: Superstore Orders Analysis
  layout: newspaper
  preferred_viewer: dashboards-next

  filters:
  - name: date_filter
    title: Date Range
    type: date_filter
    default_value: "90 days"
    allow_multiple_values: false
    required: false

  - name: region_filter
    title: Region
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    model: superstore
    explore: orders
    field: orders_derived.region

  - name: segment_filter
    title: Segment
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    model: superstore
    explore: orders
    field: orders_derived.segment

  - name: category_filter
    title: Category
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    model: superstore
    explore: orders
    field: orders_derived.category

  elements:
  - title: Total Sales
    name: total_sales_scorecard
    model: superstore
    explore: orders
    type: single_value
    fields: [orders_derived.total_sales]
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 0
    col: 0
    width: 6
    height: 4

  - title: Total Profit
    name: total_profit_scorecard
    model: superstore
    explore: orders
    type: single_value
    fields: [orders_derived.total_profit]
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 0
    col: 6
    width: 6
    height: 4

  - title: Number of Orders
    name: order_count_scorecard
    model: superstore
    explore: orders
    type: single_value
    fields: [orders_derived.order_count]
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 0
    col: 12
    width: 6
    height: 4

  - title: Weighted Profit Margin
    name: profit_margin_scorecard
    model: superstore
    explore: orders
    type: single_value
    fields: [orders_derived.weighted_profit_margin]
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 0
    col: 18
    width: 6
    height: 4

  - title: Sales by Region
    name: sales_by_region
    model: superstore
    explore: orders
    type: looker_bar
    fields: [orders_derived.region, orders_derived.total_sales]
    sorts: [orders_derived.total_sales desc]
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 4
    col: 0
    width: 12
    height: 8

  - title: Sales by Category
    name: sales_by_category
    model: superstore
    explore: orders
    type: looker_bar
    fields: [orders_derived.category, orders_derived.total_sales]
    sorts: [orders_derived.total_sales desc]
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 4
    col: 12
    width: 12
    height: 8

  - title: Sales by Customer Segment
    name: sales_by_segment
    model: superstore
    explore: orders
    type: looker_bar
    fields: [orders_derived.segment, orders_derived.total_sales]
    sorts: [orders_derived.total_sales desc]
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 12
    col: 0
    width: 12
    height: 8

  - title: Top 10 Customers by Sales
    name: top_customers_table
    model: superstore
    explore: orders
    type: looker_grid
    fields: [orders_derived.customer_name, orders_derived.total_sales, orders_derived.total_profit, orders_derived.order_count]
    sorts: [orders_derived.total_sales desc]
    limit: 10
    listen:
      date_filter: orders_derived.order_date
      region_filter: orders_derived.region
      segment_filter: orders_derived.segment
      category_filter: orders_derived.category
    row: 12
    col: 12
    width: 12
    height: 8