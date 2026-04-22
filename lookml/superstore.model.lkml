connection: "bigquery_gothic_sum"

label: "Superstore"

include: "/lookml/views/*.view.lkml"

explore: sales_summary {
  label: "Sales"
  description: "Sales performance by date, region, category, and segment"
  from: mart_sales_summary
}