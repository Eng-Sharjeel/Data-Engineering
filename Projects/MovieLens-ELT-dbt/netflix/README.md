## 📁 dbt Project Structure

A DBT project consists of several key folders, each serving a distinct purpose in transforming raw data into analytics-ready tables. This section explains what each folder typically contains and how it’s used in dbt workflows.

### 📦 `models/` — Core Transformations
The `models/` directory contains **dbt models**, which are SQL files used to transform raw data into derived tables or views. Models are compiled and run by dbt to build structures in your data warehouse.

Within `models/`, it’s common to organize by purpose:

- **staging/** — Staging models that clean and prepare raw source data  
- **dim/** — Dimension tables for descriptive attributes (dev layer)  
- **fact/** — Fact tables for measurable events  
- **mart/** — Analytics-ready models and aggregated views  

Each model is essentially a `SELECT` query that dbt runs against your source data.

### 🕒 `snapshots/` — Historical Tracking (SCD Type 2)
The `snapshots/` folder contains snapshot definitions used to capture changes in your data over time.  
Snapshots allow you to implement **Slowly Changing Dimension Type 2 (SCD Type 2)** logic — dbt tracks the full change history of a record, maintaining versions as they evolve.

### 🧠 `macros/` — Reusable SQL Logic
The `macros/` folder holds **reusable SQL functions written in Jinja**. Macros simplify complex or repetitive logic so you can call them from multiple models, making your code more modular and maintainable.

### 📊 `seeds/` — CSV Reference Data
Files in the `seeds/` directory are simple **CSV files** that dbt can load directly into your warehouse as tables.  
Seeds are best suited for static lookup/reference data that rarely changes (e.g., mappings or metadata), and can be referenced just like regular models using `ref()`.

### 📓 `analyses/` — Explorer Queries
Files in the `analyses/` folder are **SQL scripts used for ad-hoc or exploratory queries**. These are compiled by dbt (but not run automatically) and can be used for reporting, experimentation, or deeper data investigation.

### 📦 `target/` — Compiled Artifacts (Auto-Generated)
The `target/` directory is automatically created by dbt when you run or compile models. It contains:

- Compiled SQL files (the code dbt actually runs)  
- Run results and artifacts  

This folder is **not tracked in Git**, as its contents are generated during dbt runs.

### 📜 `logs/` — Execution Logs (Auto-Generated)
Whenever dbt runs (e.g., `dbt run`, `dbt test`), it writes log files here. These logs help with debugging and understanding pipeline execution but are **not committed to Git**.
