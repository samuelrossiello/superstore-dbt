- dashboard: executive_summary
  title: Superstore Executive Summary
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
    explore: sales_summary
    field: mart_sales_summary.region

  - name: segment_filter
    title: Segment
    type: field_filter
    default_value: ''
    allow_multiple_values: true
    required: false
    model: superstore
    explore: sales_summary
    field: mart_sales_summary.segment

  elements:
  - title: Total Sales
    name: total_sales_scorecard
    model: superstore
    explore: sales_summary
    type: single_value
    fields: [mart_sales_summary.total_sales]
    listen:
      date_filter: mart_sales_summary.order_date
      region_filter: mart_sales_summary.region
      segment_filter: mart_sales_summary.segment
    row: 0
    col: 0
    width: 6
    height: 4

  - title: Total Profit
    name: total_profit_scorecard
    model: superstore
    explore: sales_summary
    type: single_value
    fields: [mart_sales_summary.total_profit]
    listen:
      date_filter: mart_sales_summary.order_date
      region_filter: mart_sales_summary.region
      segment_filter: mart_sales_summary.segment
    row: 0
    col: 6
    width: 6
    height: 4

  - title: Total Units Sold
    name: total_quantity_scorecard
    model: superstore
    explore: sales_summary
    type: single_value
    fields: [mart_sales_summary.total_quantity]
    listen:
      date_filter: mart_sales_summary.order_date
      region_filter: mart_sales_summary.region
      segment_filter: mart_sales_summary.segment
    row: 0
    col: 12
    width: 6
    height: 4

  - title: Weighted Profit Margin
    name: profit_margin_scorecard
    model: superstore
    explore: sales_summary
    type: single_value
    fields: [mart_sales_summary.weighted_profit_margin]
    listen:
      date_filter: mart_sales_summary.order_date
      region_filter: mart_sales_summary.region
      segment_filter: mart_sales_summary.segment
    row: 0
    col: 18
    width: 6
    height: 4

  - title: Monthly Sales Trend
    name: monthly_sales_trend
    model: superstore
    explore: sales_summary
    type: looker_line
    fields: [mart_sales_summary.order_date, mart_sales_summary.total_sales]
    sorts: [mart_sales_summary.order_date asc]
    listen:
      date_filter: mart_sales_summary.order_date
      region_filter: mart_sales_summary.region
      segment_filter: mart_sales_summary.segment
    row: 4
    col: 0
    width: 24
    height: 8

  - title: Sales by Region
    name: sales_by_region
    model: superstore
    explore: sales_summary
    type: looker_bar
    fields: [mart_sales_summary.region, mart_sales_summary.total_sales]
    sorts: [mart_sales_summary.total_sales desc]
    listen:
      date_filter: mart_sales_summary.order_date
      region_filter: mart_sales_summary.region
      segment_filter: mart_sales_summary.segment
    row: 12
    col: 0
    width: 12
    height: 8

  - title: Sales by Category
    name: sales_by_category
    model: superstore
    explore: sales_summary
    type: looker_bar
    fields: [mart_sales_summary.category, mart_sales_summary.total_sales]
    sorts: [mart_sales_summary.total_sales desc]
    listen:
      date_filter: mart_sales_summary.order_date
      region_filter: mart_sales_summary.region
      segment_filter: mart_sales_summary.segment
    row: 12
    col: 12
    width: 12
    height: 8