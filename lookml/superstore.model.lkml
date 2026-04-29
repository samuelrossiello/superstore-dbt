connection: "bigquery_gothic_sum"

label: "Superstore"

include: "/lookml/views/*.view.lkml"

explore: sales_summary {
  label: "Sales"
  description: "Sales performance by date, region, category, and segment"
  from: mart_sales_summary
}

explore: customer_summary {
  label: "Customers"
  description: "Customer behavior, lifetime metrics, and preferred category"
  from: mart_customer_summary
}

explore: orders {
  label: "Orders"
  description: "Order-level detail with customer context, enabling joined analysis across sales and customer data"
  from: orders_derived

  join: mart_customer_summary {
    type: left_outer
    sql_on: ${orders_derived.customer_id} = ${mart_customer_summary.customer_id} ;;
    relationship: many_to_one
  }
}