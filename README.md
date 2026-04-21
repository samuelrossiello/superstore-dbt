# Superstore Pipeline Dashboard

A end-to-end business intelligence project built on a fully cloud-native ELT pipeline, transforming raw Superstore transactional data into three interactive Tableau dashboards covering executive performance, customer analytics, and cohort retention analysis.

**Live Dashboard:** [Superstore Pipeline Dashboard on Tableau Public](https://public.tableau.com/app/profile/samuel.rossiello/viz/SuperstorePipelineDashboard/Dashboard1)

---

## Project Overview

This project answers three core analytical questions:

1. **Executive Summary** — How is the business performing across regions, categories, and time? Where are we profitable and where are we not?
2. **Customer Analytics** — Who are our most valuable customers? How are they distributed by lifetime sales, order frequency, and segment?
3. **Cohort Analysis** — Are we retaining customers after acquisition? Which cohorts retain best, and how quickly do customers churn after their first purchase?

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
        │  Google Sheets bridge
        ▼
Tableau Public
  3-dashboard workbook
```

**Why ELT over ETL?** Transformations happen inside BigQuery after loading, keeping raw data intact and making the pipeline easier to debug, version, and iterate on. dbt handles all transformation logic with version-controlled SQL models.

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

**4. Open the workbook**
- The published workbook is available at the Tableau Public link above
- All three dashboards are included with navigation buttons to move between them

---

## Dashboards

### Dashboard 1 — Executive Summary
An interactive overview of Superstore sales performance and profitability. Features a dual-encoded geographic bubble map, dynamic trend line with metric toggle parameter, profit by category bar chart, and a sales by category and region heatmap.

<img width="1180" height="679" alt="image" src="https://github.com/user-attachments/assets/764913bd-b4ac-4845-9077-774d05b56992" />

### Dashboard 2 — Customer Analytics
An interactive exploration of customer behavior, lifetime value, and purchasing patterns. Features a dynamic scatter plot explorer with X/Y axis parameters, lifetime sales distribution by segment, order frequency distribution, and a top 15 customers ranked bar chart.

<img width="1185" height="688" alt="image" src="https://github.com/user-attachments/assets/34bd4c7a-f3ac-4fa6-b835-1bb0107cb557" />

### Dashboard 3 — Cohort Analysis
An interactive exploration of customer retention patterns and acquisition trends by cohort month. Features a cohort retention heatmap built with a FIXED LOD expression, a retention decay curve, customer acquisition by cohort month, and average sales per customer by segment.

<img width="1178" height="688" alt="image" src="https://github.com/user-attachments/assets/091e93a6-5c63-42e7-b5a8-0b8487068b97" />

---

## Technical Highlights

- **FIXED LOD expressions** — Used to anchor cohort baseline customer counts independent of view-level filters, enabling accurate retention rate calculation across all period numbers
- **Parameter-driven visualizations** — Metric toggle parameters on Dashboards 1 and 2 allow users to switch between measures without navigating to a new view
- **Cloud-native ELT** — All transformation logic lives in version-controlled dbt SQL models running inside BigQuery, with no local dependencies
- **Cohort analysis methodology** — Retention rate calculated as active customers in period N divided by original cohort size at period 0, with minimum cohort size filtering to remove statistically unreliable small cohorts
- **Stephen Few design principles** — Sequential color palettes for ordinal data, color-blind-safe palette choices, minimal chart decoration, and purposeful use of color encoding throughout

---

## Tools & Technologies

| Layer | Tool |
|---|---|
| Data ingestion & cleaning | Python (pandas), Google Colab |
| Cloud data warehouse | Google BigQuery |
| Transformation | dbt Cloud (Developer tier) |
| Visualization | Tableau Public |
| Version control | GitHub (branch/PR/merge workflow) |

---

## Author

Samuel Rossiello
[Tableau Public Profile](https://public.tableau.com/app/profile/samuel.rossiello)
[GitHub](https://github.com/samuelrossiello/superstore-dbt)
