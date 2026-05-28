# Superstore Pipeline Dashboard

A end-to-end business intelligence project built on a fully cloud-native ELT pipeline, transforming raw Superstore transactional data into three interactive Tableau dashboards covering executive performance, customer analytics, and cohort retention analysis.

**Tableau Dashboard:** [Superstore Pipeline Dashboard on Tableau Public](https://public.tableau.com/app/profile/samuel.rossiello/viz/SuperstorePipelineDashboard/Dashboard1)

**Looker Studio Dashboard:** [Superstore Analytics Dashboard on Looker Studio](https://datastudio.google.com/reporting/3ec2b564-947f-4d68-ad16-76a0e7573cfc)

---

## Project Overview

This project answers three core analytical questions:

1. **Executive Summary** — How is the business performing across regions, categories, and time? Where are we profitable and where are we not?
2. **Customer Analytics** — Who are our most valuable customers? How are they distributed by lifetime sales, order frequency, and segment?
3. **Sales Performance Trends** — How are sales trending over time? Are we growing year over year, and which time periods are driving the most growth?
4. **Cohort Analysis** — Are we retaining customers after acquisition? Which cohorts retain best, and how quickly do customers churn after their first purchase?

The project demonstrates a complete modern data stack workflow — from raw data ingestion through cloud transformation to published interactive dashboards — using entirely free-tier cloud tooling.

---

## Pipeline Architecture

```
Google Colab (Python/pandas)
        │
        │  Data cleaning & loading
        ▼
Google BigQuery
  Project: gothic-sum-492014-r0
  Dataset: dbt_srossiello
        │
        │  SQL transformations
        ▼
dbt Cloud (Developer tier)
  Staging → Mart models
        │
        ├─────────────────────────────────┐
        │                                 │
        │  Google Sheets bridge           │  LookML semantic layer
        ▼                                 ▼
Tableau Public                     Looker Studio
  3-dashboard workbook               4-page dashboard suite
  Executive Summary                  Executive Summary
  Customer Analytics                 Customer Analytics
  Cohort Analysis                    Sales Performance Trends
                                     Cohort Analysis
```

**Why ELT over ETL?** Transformations happen inside BigQuery after loading, keeping raw data intact and making the pipeline easier to debug, version, and iterate on. dbt handles all transformation logic with version-controlled SQL models.

**Why a semantic layer?** LookML sits between BigQuery and the visualization layer, defining business logic — metric definitions, aggregation rules, join relationships — once and enforcing it consistently across every dashboard, every query, and every user. Without a semantic layer, each report author defines metrics independently, creating inconsistency at scale.

---

## Repository Structure

```
superstore-dbt/
├── models/
│   ├── staging/
│   │   └── stg_orders.sql
│   └── marts/
│       ├── mart_sales_summary.sql
│       ├── mart_customer_summary.sql
│       └── mart_cohort_analysis.sql
├── lookml/
│   ├── superstore.model.lkml
│   ├── views/
│   │   ├── mart_sales_summary.view.lkml
│   │   ├── mart_customer_summary.view.lkml
│   │   └── orders_derived.view.lkml
│   └── dashboards/
│       ├── executive_summary.dashboard.lkml
│       ├── customer_analytics.dashboard.lkml
│       └── orders.dashboard.lkml
├── dbt_project.yml
└── README.md
```

---

## Model Descriptions

### `stg_orders`
The staging layer. Reads from the raw Superstore source table in BigQuery and applies type casting, column renaming, and basic cleaning. All downstream models reference this staging model rather than the raw source, ensuring a single point of truth for data quality logic.

### `mart_sales_summary`
Aggregated sales and profit metrics grouped by date, region, state, category, sub-category, and segment. Powers the Executive Summary dashboard — maps, trend lines, category breakdowns, and the regional sales heatmap.

**Grain:** One row per date / region / state / category / sub-category / segment combination.

### `mart_customer_summary`
One row per customer, enriched with lifetime metrics. Key fields include `lifetime_sales`, `lifetime_profit`, `lifetime_profit_margin`, `order_count`, `avg_order_value`, `avg_days_between_orders`, and `preferred_category`.

**Grain:** One row per customer.

### `mart_cohort_analysis`
Cohort metrics aggregated by acquisition month, period number, and segment. `cohort_month` is defined as the month of each customer's first purchase. `period_number` is the number of months elapsed since that first purchase. Powers the Cohort Analysis dashboard including the retention heatmap and retention curve.

**Grain:** One row per cohort month / period number / segment combination.

---

## LookML Semantic Layer

The LookML project defines a semantic layer on top of the dbt mart models, governing how BigQuery data is exposed to end users. All LookML files live in the `/lookml` folder of this repository and would be deployed to a Looker instance in a production environment.

### Model File — `superstore.model.lkml`
Defines the BigQuery connection and three explores available to end users:
- **Sales** — built on `mart_sales_summary`, covering sales performance by date, region, category, and segment
- **Customers** — built on `mart_customer_summary`, covering customer behavior, lifetime metrics, and acquisition cohorts
- **Orders** — built on `orders_derived` with a join to `mart_customer_summary`, enabling order-level analysis with customer context

### View Files

**`mart_sales_summary.view.lkml`**
Exposes the sales mart as a governed LookML view with 15+ dimensions and measures organized into labeled groups. Key features:
- `type: sum_distinct` measure for fanout-safe aggregation in join scenarios
- Filtered measure (`furniture_sales`) scoping a metric to a specific category without a subquery
- `type: number` measure (`weighted_profit_margin`) computing true weighted margin as `SUM(profit) / SUM(sales)` — avoids the average of averages problem
- Dynamic grouping parameter (`group_by_selector`) with Liquid templating — lets users switch GROUP BY between region, category, segment, and state without rebuilding the query
- Date granularity parameter (`date_granularity`) — lets users switch between day, month, quarter, and year aggregation dynamically
- Period-over-period measures (`total_sales_prior_period`, `sales_period_over_period_change`) for year-over-year comparison
- `type: running_total` measure for cumulative sales analysis

**`mart_customer_summary.view.lkml`**
Exposes the customer mart with customer-level dimensions and measures. Key features:
- `primary_key: yes` on `customer_id` enabling safe join behavior and symmetric aggregates
- `customer_acquisition_cohort` dimension using CASE WHEN on `first_order_date` — true acquisition cohort assigning customers to yearly buckets based on first purchase
- `is_high_value_customer` yesno dimension and `order_count_tier` tier dimension for behavioral segmentation

**`orders_derived.view.lkml`**
A LookML derived table querying `stg_orders` directly to produce an order-level summary that preserves `customer_id` alongside sales metrics. This solves a key architectural challenge: `mart_sales_summary` aggregates `customer_id` away, making a direct join to `mart_customer_summary` impossible. The derived table bridges that gap by going back to the staging layer.

### Explores and Join Logic
The Orders explore joins `orders_derived` to `mart_customer_summary` on `customer_id` using `relationship: many_to_one` — many orders belong to one customer. This is the only explore in the project that combines order-level sales data with customer-level attributes in a single query.

### LookML Dashboard Definitions
Three YAML dashboard files define governed, version-controlled dashboard specifications:
- `executive_summary.dashboard.lkml` — four scorecards, monthly trend, sales by region and category
- `customer_analytics.dashboard.lkml` — customer KPIs, segment analysis, top customers table
- `orders.dashboard.lkml` — order-level analysis using the joined Orders explore

All dashboard tiles use `listen` blocks to connect dashboard filters to specific LookML fields — ensuring filter behavior is governed in code rather than configured manually per report.

### Known Limitations
- **Average of averages:** `avg_order_value` and `avg_days_between_orders` average pre-aggregated mart values. A weighted average would require raw order line data from `stg_orders`.
- **No live Looker instance:** LookML files are authored in dbt Cloud's IDE as a workaround. In production, LookML would live in a separate repo connected to Looker's built-in IDE.
- **Date filter on Customer Analytics:** The date range filter uses `first_order_date` as a proxy — a known limitation noted in the dashboard itself.
- **Dynamic GROUP BY not replicable in Looker Studio:** LookML parameters change the SQL GROUP BY at query time. Looker Studio filters apply post-query and cannot replicate this behavior.

---

## Key Calculated Fields (Tableau)

### Cohort Size (FIXED LOD)
```
{ FIXED [Cohort Month] : SUM(IF [Period Number] = 0 THEN [Customer Count] END) }
```
Anchors the cohort baseline (period 0 customer count) to each cohort month, regardless of what period is currently in view. Used as the denominator for retention rate.

### Retention Rate
```
SUM([Customer Count]) / MIN([Cohort Size])
```
The percentage of a cohort's original customers who made a purchase in a given period.

### Avg Retention Rate
```
SUM([Customer Count]) / SUM([Cohort Size])
```
Used in the Retention Curve to average retention across all cohorts at each period number.

---

## How to Reproduce

### Prerequisites
- Google account (for Colab and BigQuery)
- dbt Cloud account (free Developer tier)
- Tableau Public account (free)
- Looker Studio account (free, via Google account)

### Steps

**1. Load raw data into BigQuery**
- Open the data loading notebook in Google Colab (saved in Google Drive under "Python & Visualizations Bootcamp")
- Run all cells to clean the raw Superstore CSV with pandas and load it into BigQuery project `gothic-sum-492014-r0`, dataset `dbt_srossiello`

**2. Run dbt transformations**
- Clone this repository and connect it to dbt Cloud
- Run `dbt run` to build all staging and mart models in BigQuery
- Run `dbt test` to validate model outputs

**3. Connect Tableau to BigQuery**
- In Tableau Public, connect to Google BigQuery using the `dbt_srossiello` dataset
- Add `mart_sales_summary`, `mart_customer_summary`, and `mart_cohort_analysis` as data sources
- Use Google Sheets as a bridge layer if direct BigQuery connection is unavailable in Tableau Public

**4. Open the Tableau workbook**
- The published workbook is available at the Tableau Public link above
- All three dashboards are included with navigation buttons to move between them

**5. Review the LookML semantic layer**
- The `/lookml` folder contains the full LookML project — model file, view files, and dashboard definitions
- In a production environment these files would be deployed to a Looker instance connected to BigQuery
- The LookML was authored in dbt Cloud's IDE as a workaround — in production it would live in a separate repo connected to Looker's built-in IDE

**6. Open the Looker Studio report**
- Go to the Looker Studio link above
- The report connects directly to the dbt mart models in BigQuery — no additional setup required
- All four pages are available with interactive filters

---

## Dashboards

### Tableau Public — Superstore Pipeline Dashboard

#### Dashboard 1 — Executive Summary
An interactive overview of Superstore sales performance and profitability. Features a dual-encoded geographic bubble map, dynamic trend line with metric toggle parameter, profit by category bar chart, and a sales by category and region heatmap.

<img width="1180" height="679" alt="image" src="https://github.com/user-attachments/assets/764913bd-b4ac-4845-9077-774d05b56992" />

#### Dashboard 2 — Customer Analytics
An interactive exploration of customer behavior, lifetime value, and purchasing patterns. Features a dynamic scatter plot explorer with X/Y axis parameters, lifetime sales distribution by segment, order frequency distribution, and a top 15 customers ranked bar chart.

<img width="1185" height="688" alt="image" src="https://github.com/user-attachments/assets/34bd4c7a-f3ac-4fa6-b835-1bb0107cb557" />

#### Dashboard 3 — Cohort Analysis
An interactive exploration of customer retention patterns and acquisition trends by cohort month. Features a cohort retention heatmap built with a FIXED LOD expression, a retention decay curve, customer acquisition by cohort month, and average sales per customer by segment.

<img width="1178" height="688" alt="image" src="https://github.com/user-attachments/assets/091e93a6-5c63-42e7-b5a8-0b8487068b97" />

---

### Looker Studio — Superstore Analytics Dashboard

#### Page 1 — Executive Summary
Sales performance overview powered by `mart_sales_summary`. Four KPI scorecards with total sales, profit, units sold, and weighted profit margin. Monthly sales trend line chart and sales by region and category horizontal bar charts. Cross-page filters for date range, region, and segment.

<img width="962" height="721" alt="image" src="https://github.com/user-attachments/assets/fb29ebd2-f7da-45ef-83c1-66cce9a04a5f" />

#### Page 2 — Customer Analytics
Customer behavior analysis powered by `mart_customer_summary`. Four customer KPI scorecards, average order value by segment, customers by preferred category bar chart, and a top 10 customers by lifetime sales table. Date filter uses `first_order_date` as a proxy — noted as a known limitation on the dashboard itself.

<img width="960" height="718" alt="image" src="https://github.com/user-attachments/assets/26352f37-82d8-440b-8f40-aad0fbbf8a39" />

#### Page 3 — Sales Performance Trends
Period-over-period analysis powered by `mart_sales_summary`. Four KPI scorecards with dynamic year-over-year comparison deltas, monthly sales trend line chart, and cumulative sales area chart. Date range filter drives the PoP comparison automatically.

<img width="957" height="717" alt="image" src="https://github.com/user-attachments/assets/7f959844-e60a-45d2-be63-ca00264dfc1b" />

#### Page 4 — Cohort Analysis
Full historical cohort analysis with no date filter applied by design. Customers by acquisition cohort grouped by first order year, and sales by order cohort grouped by order year. Powered by both `mart_customer_summary` and `mart_sales_summary`.

<img width="958" height="722" alt="image" src="https://github.com/user-attachments/assets/1132f07e-5d70-497c-986c-581aac7b9f78" />

---

## Technical Highlights

### Tableau
- **FIXED LOD expressions** — Used to anchor cohort baseline customer counts independent of view-level filters, enabling accurate retention rate calculation across all period numbers
- **Parameter-driven visualizations** — Metric toggle parameters on Dashboards 1 and 2 allow users to switch between measures without navigating to a new view
- **Cloud-native ELT** — All transformation logic lives in version-controlled dbt SQL models running inside BigQuery, with no local dependencies
- **Cohort analysis methodology** — Retention rate calculated as active customers in period N divided by original cohort size at period 0, with minimum cohort size filtering to remove statistically unreliable small cohorts
- **Stephen Few design principles** — Sequential color palettes for ordinal data, color-blind-safe palette choices, minimal chart decoration, and purposeful use of color encoding throughout

### LookML Semantic Layer
- **Governed metric definitions** — Dimensions and measures defined once in LookML and enforced consistently across every explore, dashboard, and API call. No per-report metric redefinition.
- **Fanout-safe aggregation** — `type: sum_distinct` and `type: count_distinct` measures handle row duplication safely in join scenarios, using `primary_key` declarations for deduplication
- **Weighted profit margin** — `type: number` measure computing `SUM(profit) / NULLIF(SUM(sales), 0)` dynamically at query time — avoids the average of averages problem inherent in averaging pre-aggregated mart values
- **Liquid templating** — Dynamic GROUP BY parameter lets users switch between region, category, segment, and state groupings, changing the actual SQL at query time rather than filtering post-query
- **Derived table join architecture** — `orders_derived` solves a key grain mismatch: `mart_sales_summary` aggregates `customer_id` away, making a direct join to `mart_customer_summary` impossible. The derived table queries `stg_orders` directly to preserve `customer_id` at order level
- **Production-quality field organization** — All view files use `group_label`, `description`, and `hidden` properties to produce a navigable, self-documenting field picker
- **Version-controlled dashboard definitions** — Three YAML dashboard files define tile layout, filter configuration, and listen block mappings in code — deployable through CI/CD rather than manual UI configuration

### Known Limitations
- **Average of averages:** `avg_order_value` and `avg_days_between_orders` average pre-aggregated mart values. A weighted average would require raw order line data from `stg_orders`
- **No live Looker instance:** LookML files authored in dbt Cloud's IDE as a workaround. In production, LookML would live in a separate repo connected to Looker's built-in IDE
- **Date filter on Customer Analytics:** Date range filter uses `first_order_date` as a proxy — noted on the dashboard itself
- **Dynamic GROUP BY not replicable in Looker Studio:** LookML parameters change the SQL GROUP BY at query time. Looker Studio filters apply post-query and cannot replicate this behavior
- **Single-metric category filter not replicable in Looker Studio:** LookML templated filters scope one measure without affecting others. Looker Studio filter controls apply globally to all charts sharing the same data source

### Cross-Tool Comparison

| Capability | Tableau | Looker Studio | Looker |
|---|---|---|---|
| Governed metric definitions | Workbook-level | Per-report | Semantic layer — universal |
| Dynamic GROUP BY via parameter | Yes (parameters) | No | Yes (LookML parameters) |
| Version-controlled dashboards | No | No | Yes (LookML YAML) |
| Free persistent access | Yes (Tableau Public) | Yes | No (enterprise only) |
| Native dbt integration | Via Google Sheets bridge | Via BigQuery | Native semantic layer alignment |
| Self-service exploration | Limited | Limited | Full (governed explores) |
| Setup complexity | Medium | Low | High |

**When to recommend each:**
- **Tableau** — Rich, highly customized visualizations for polished client-facing deliverables. Best when visual design control matters most.
- **Looker Studio** — Quick, free dashboards connected to Google Cloud data. Best for lightweight reporting without governance requirements.
- **Looker** — Enterprise self-service analytics requiring governed metric definitions, consistent business logic, and scalable data access across large teams.

---

## Tools & Technologies

| Layer | Tool |
|---|---|
| Data ingestion & cleaning | Python (pandas), Google Colab |
| Cloud data warehouse | Google BigQuery |
| Transformation | dbt Cloud (Developer tier) |
| Semantic layer | LookML (authored in dbt Cloud IDE) |
| Visualization — Tableau | Tableau Public |
| Visualization — Looker Studio | Google Looker Studio |
| Version control | GitHub (branch/PR/merge workflow) |

---

## Author

**Samuel Rossiello**

- GitHub: [github.com/samuelrossiello](https://github.com/samuelrossiello)
- Tableau Public: [public.tableau.com/profile/samuel.rossiello](https://public.tableau.com/app/profile/samuel.rossiello)
