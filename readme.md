# SQL Data Warehouse Project (Medallion Architecture)

This project demonstrates the end-to-end design and implementation of a **Data Warehouse** using the **Medallion Architecture** — Bronze, Silver, and Gold layers, entirely built with **SQL Server**.  
It covers data ingestion, transformation, quality checks, modeling, and documentation, reflecting a real-world data engineering workflow.

---

## 🧱 Project Architecture

The repository follows the **Medallion Architecture** pattern:

| Layer | Purpose | Key Deliverables |
|-------|----------|------------------|
| **Bronze** | Ingest and store raw source data as-is. | Schema creation, stored procedures, data loading scripts. |
| **Silver** | Cleanse, validate, and standardize data. | Data transformation, enrichment, and metadata management. |
| **Gold** | Model data for analytics and business reporting. | Fact and dimension tables, star schema, and integration checks. |

📄 Architecture, flow, and schema visuals are available under `Supporting Docs and Images`.

---

## 📂 Repository Structure
```bash
SQL-DataWarehouse/
│
├── Code/
│ ├── Bronze/ # Raw data ingestion layer
│ │ ├── Database_and_Schema_Creation.sql
│ │ ├── Table_Creation_DDL_bronze.sql
│ │ └── Data_Loading_Stored_Procedure.sql
│ │
│ ├── Silver/ # Cleaned & transformed layer
│ │ ├── Table_scripts_crm/
│ │ │ ├── Table_Creation_DDL_Silver.sql
│ │ │ ├── Table_silver_crm_cust_info.sql
│ │ │ └── Table_silver_crm_prd_info.sql
│ │ ├── Table_scripts_erp/
│ │ │ ├── Data_loading_storedprocedure.sql
│ │ │ └── Table_Creation_DDL_Silver.sql
│ │
│ ├── Gold/ # Business-ready analytical layer
│ │ ├── Gold_DDL_script.sql
│ │ └── Views scripts/
│ │ ├── Gold_dimension_customer.sql
│ │ ├── Gold_dimension_product.sql
│ │ └── Gold_fact_sales.sql
│ │
│ ├── Quality check Scripts/
│ │ ├── Gold_quality_check.sql
│ │ └── Silver_quality_check.sql
│
├── Supporting Docs and Images/
│ ├── Data Architecture.png
│ ├── Data Flow.png
│ ├── Schema Diagram.png
│ ├── Project Notes.pdf
│ └── Data Integration.png
│
├── datasets/
│ ├── source_crm/
│ │ ├── cust_info.csv
│ │ ├── prd_info.csv
│ │ └── sales_details.csv
│ ├── source_erp/
│ │ ├── CUST_AZ12.csv
│ │ └── LOC_A101.csv
│
└── README.md
```
---

## ⚙️ Bronze Layer — Raw Data Ingestion

**Goal:** Capture and store raw data from CRM and ERP systems without transformation.

**Steps:**
1. **Analyze** source structure — understand fields, data types, and constraints.  
2. **Code**:  
   - Create schemas and DDL scripts.  
   - Use `BULK INSERT` for ingestion.  
   - Implement `TRY...CATCH` and message logging in stored procedures.  
3. **Validate**:  
   - Perform completeness and schema checks.  
   - Validate row counts post-load.  
4. **Version & Document** everything in Git.

**Additional considerations:**
- Define business ownership, data documentation, and system dependencies.  
- Evaluate architecture, integration capabilities, and load strategies (incremental vs full).  
- Add authentication and performance controls to minimize source impact.

---

## ⚙️ Silver Layer — Cleansed & Standardized Data

**Goal:** Transform raw Bronze data into clean, structured, and quality-assured tables ready for modeling.

**Steps:**
1. **Analyze:** Explore data relationships and visualize tables in Draw.io.  
2. **Code:**
   - Copy DDLs from Bronze to Silver schema.
   - Add metadata columns (`create_date`, `update_date`, `source_system`, `file_location`).
   - Clean duplicates, handle nulls, trim whitespaces, and standardize categorical values.  
   - Perform transformations (CASE, SUBSTRING, CAST, etc.).  
3. **Validate:**  
   - Run data correctness and consistency checks via Silver quality scripts.  
4. **Documentation:**  
   - Maintain consistent naming, log load times, and track transformations in Git.

**Example Enhancements:**
- Derived columns (e.g., `sales = quantity * price`)  
- Data enrichment for date validations and product category normalization  
- Stored procedure automation with print messages, load duration, and error handling  

---

## ⚙️ Gold Layer — Business-Ready Data

**Goal:** Integrate cleaned data into a **star schema** for analytics.

**Steps:**
1. **Analyze Business Objects:** Identify entities like Customers, Products, and Sales.  
2. **Code:**
   - Integrate data from Silver views into **Fact** and **Dimension** tables.  
   - Create surrogate keys using SQL window functions (`ROW_NUMBER()`).  
   - Use dimension keys instead of source IDs to establish relationships.  
3. **Validate:**
   - Check data integration integrity and foreign key consistency.  
4. **Document & Version:**  
   - Maintain documentation and schema diagrams for business consumption.

**Data Modeling:**
- **Customers Dimension:** Consolidates customer attributes from multiple tables.  
- **Products Dimension:** Maintains product hierarchy and current items only.  
- **Fact Sales:** Connects multiple dimensions via surrogate keys, representing sales metrics.

**Post-Modeling Steps:**
- Verify referential integrity.  
- Extend data flow and catalog documentation.  
- Update DDL scripts if derived columns were added later.

---

## 🧩 Quality & Validation

Each transformation stage is validated through:
- Schema and completeness checks (Bronze)  
- Consistency and duplication checks (Silver)  
- Integrity and relationship checks (Gold)

Scripts under `Quality check Scripts` automate these verifications.

---

## 🧠 Concepts Applied

- Medallion Architecture (Bronze → Silver → Gold)
- ETL and Stored Procedure Design
- Bulk Insert Operations
- Metadata Management
- Surrogate Key Generation
- Star Schema Modeling
- Git Version Control and Documentation
- SQL-Based Quality Validation

---

## 🛠️ Tools & Setup

| Tool | Purpose |
|------|----------|
| **SQL Server Express** | Database engine |
| **SQL Server Management Studio (SSMS)** | Query and ETL script execution |
| **Draw.io** | Data flow and model visualization |
| **Git & GitHub** | Versioning and documentation |
| **CSV Datasets** | Source data (ERP + CRM) |

---

## 🧾 Documentation

All supporting documentation — including schema diagrams, data flow, and architecture — can be found in  
`Supporting Docs and Images/Project Notes.pdf`.

---

## 🧑‍💻 Author

**Shivam Kumar**  
Data Analyst | Writer 

> Building bridges between data pipelines and human insight.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/meshivamk/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github)](https://github.com/meshivamk)
