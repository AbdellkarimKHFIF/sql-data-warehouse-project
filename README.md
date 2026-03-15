🚀 Modern Data Warehouse with SQL Server

📌 Project Overview

This project demonstrates the design and implementation of a **Modern Data Warehouse** using **Microsoft SQL Server**.

The objective of this project is to simulate a **real-world data engineering workflow**, including:

* Ingesting raw data from multiple source systems
* Building an **ETL pipeline** using T-SQL
* Cleaning and transforming data
* Designing a **Star Schema data model**
* Creating **analytics-ready datasets** for business reporting

The warehouse integrates data from **CRM** and **ERP systems** and transforms it into **business-ready insights** used for analytics and decision-making.

---

🎯 Project Goals

The main goals of this project are:

* Practice **Data Warehouse architecture design**
* Build a **multi-layer data pipeline**
* Implement **data cleaning and transformation logic**
* Model data using **fact and dimension tables**
* Perform **business analytics using SQL**

---

🏗 Data Warehouse Architecture

This project follows the **Medallion Architecture**, a modern data engineering pattern used to organize data into multiple layers that improve **data quality, reliability, and performance**.

The architecture is divided into three layers:

* **Bronze Layer** → Raw data ingestion
* **Silver Layer** → Cleaned and transformed data
* **Gold Layer** → Business-ready analytical model

---

📊 Architecture Overview

```
        Source Systems
        (CRM / ERP)
             │
             ▼
     ┌─────────────────┐
     │   Bronze Layer  │
     │   Raw Data      │
     └─────────────────┘
             │
             ▼
     ┌─────────────────┐
     │   Silver Layer  │
     │  Cleaned Data   │
     └─────────────────┘
             │
             ▼
     ┌─────────────────┐
     │    Gold Layer   │
     │ Business Model  │
     └─────────────────┘
             │
             ▼
       Analytics / BI
```

---

🥉 Bronze Layer – Raw Data

The **Bronze layer** stores raw data ingested from source systems without transformations.

Characteristics:

* Raw data is preserved exactly as received
* No data cleaning or transformations
* Serves as the **landing zone for source data**

Data sources include:

* **CRM System**
* **ERP System**

---

🥈 Silver Layer – Cleaned Data

The **Silver layer** performs data transformation and cleaning.

Key operations include:

* Removing duplicate records
* Handling missing values
* Standardizing formats
* Data type conversions
* Validating business rules

This layer ensures that the data becomes **consistent and reliable** before entering the analytical model.

---

🥇 Gold Layer – Business Data Model

The **Gold layer** contains analytics-ready data structured for business reporting.

It includes:

* **Fact tables** for transactional data
* **Dimension tables** for descriptive attributes

This layer is optimized for:

* Business Intelligence dashboards
* Reporting
* Data analysis


---

👨‍💻 About Me

I am currently learning **Data Engineering and Data Analytics** and building projects to improve my skills.
This project is my **first data warehouse project**, where I practiced designing a **modern data warehouse using SQL Server**, implementing ETL processes, data transformations, and analytical queries.

Through this project, I learned how to work with **data pipelines, data modeling, and business analysis using SQL**. I will continue building more projects as I grow my knowledge in **data engineering, cloud technologies, and artificial intelligence**.

---
