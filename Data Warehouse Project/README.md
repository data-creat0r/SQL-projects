# Data Warehouse Project

This project showcases a comprehensive data warehousing solution: from ingesting raw source data to generating actionable insights with SQL. It’s designed as a portfolio project to demonstrate practical data engineering and analytics skills.

---

## Data Architecture

The project follows a **Medallion architecture** with three logical layers: **Bronze**, **Silver**, and **Gold**.

<img width="1228" height="674" alt="Data_architecture drawio (1)" src="https://github.com/user-attachments/assets/aba01818-edb0-484b-ba92-c43757066851" />

- **Bronze Layer**  
  - Landing zone for **raw data** coming directly from the source systems.  
  - Data is loaded from CSV files into a **SQL Server** database with no transformation.

- **Silver Layer**  
  - Focuses on **data cleaning, standardization, and basic transformations**.  
  - Handles tasks like fixing data types, removing duplicates, normalizing values, and resolving data quality issues so the data is ready for modeling.

- **Gold Layer**  
  - Contains **business-ready tables** modeled in a **star schema** (fact and dimension tables).  
  - Optimized for analytics, BI tools, and performant SQL queries used for reporting and dashboards.

---

## Project Overview

This project covers the full lifecycle of a small analytical solution:

- **Data Architecture** – Designing a modern data warehouse using the Medallion approach (Bronze, Silver, Gold).
- **ETL / ELT Pipelines** – Loading data from CSV sources, transforming it, and persisting it in the appropriate layer.
- **Data Modeling** – Creating fact and dimension tables that support analytical queries.
- **Analytics & Reporting** – Writing SQL queries to derive actionable insights.

---

## Project Requirements

### Building the Data Warehouse (Data Engineering)

**Objective**  

Create a modern data warehouse in **PostgreSQL** that consolidates sales-related data from multiple systems and supports analytical reporting.

**Scope & Specifications**

- **Data Sources**
  - Load data from two independent source systems: **ERP** and **CRM** (provided as CSV files).

- **Data Quality**
  - Detect and fix common data issues (missing values, inconsistent formats, duplicates, etc.) before data is used for analysis.

- **Integration**
  - Combine ERP and CRM datasets into a **single, integrated model** that’s easy to query for business questions.

- **Time Scope**
  - Work with the **latest available snapshot** of the data. Historical slowly changing dimensions / full historization are **out of scope** for this project.

- **Documentation**
  - Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI, Analytics & Reporting (Data Analysis)

**Objective**  

Develop SQL-based analytics to deliver detailed insights into:

- **Customer behavior**
- **Product performance**
- **Sales trends**

These analyses are implemented with **SQL queries** against the **Gold layer** and can be used as the foundation for dashboards in any BI tool.

---

## Repository Structure

```text
Data Warehouse Project/
│
├── datasets/                           # Source CSV files (ERP & CRM datasets)
│
├── docs/                               # Documentation & design assets
│   ├── Data_architecture.drawio        # Overall data architecture (Bronze/Silver/Gold)
│   ├── Data_integration.drawio         # How tables are related
│   ├── Data_lineage.drawio             # Draw.io file for the data flow diagram
│   ├── Data_model.drawio               # Draw.io file for data models (star schema)
│   ├── Layers_design.drawio            # Showing layers' purpose, transformations, and target users
│   ├── data_catalog.md                 # Data dictionary: tables, fields, and metadata
│   ├── naming-conventions.md           # Standards for naming tables, columns, and files
│
├── scripts/                            # SQL scripts by layer
│   ├── bronze/                         # Ingestion scripts (raw loads from CSV into DB)
│   ├── silver/                         # Cleaning & transformation scripts
│   ├── gold/                           # Star schema & analytical model scripts
│
├── tests/                              # Data quality checks & validation scripts
│
├── README.md                           # Project overview (this file)
```

## Stay Connected

Let's stay in touch! Feel free to connect with me on [LinkedIn](https://www.linkedin.com/in/tair-havrylo-91426b351/)
