# Data Warehouse — SQL Server (Star Schema + Stored Procedures)

> *A production-style ETL pipeline built entirely in T-SQL — transforming flat staging data into a normalized star schema through automated stored procedures.*

---

## Overview

This project demonstrates the **end-to-end design and implementation of a Data Warehouse** using **SQL Server**, built from a flat staging table (`DatawarehouseSample`) into a fully normalized **star schema** with dimension and fact tables.

The solution goes beyond basic table creation — every load and update operation is encapsulated in **reusable stored procedures**, implementing a clean and repeatable **ETL pattern** that supports both initial loads and incremental updates.

---

## Objectives

-  Design a **star schema** from a raw flat-file staging source
-  Build **dimension tables** for Customers, Employees, Products, and Dates
-  Build a **fact table** linking all dimensions with transactional sales data
-  Encapsulate all ETL logic in **stored procedures** for repeatability
-  Handle both **INSERT (new records)** and **UPDATE (changed records)** in a single procedure per table
-  Generate a **dynamic date dimension** from the actual data range

---

## Database

```sql
CREATE DATABASE DataWarehouse;
```

---

## Data Model — Star Schema

```
                    ┌─────────────────┐
                    │   dimCustomer   │
                    │  CustomerKey PK │
                    └────────┬────────┘
                             │
┌─────────────────┐  ┌───────▼────────┐  ┌─────────────────┐
│   dimEmployee   │  │   factSales    │  │   dimProducts   │
│  EmployeeKey PK ├──►  CustomerKey  ◄──┤  ProductKey  PK │
└─────────────────┘  │  EmployeeKey   │  └─────────────────┘
                     │  ProductKey    │
┌─────────────────┐  │  DateKey       │
│    dimDate      │  │  ProductPrice  │
│   DateKey PK    ◄──┤  Quantity      │
└─────────────────┘  │  OrderDate     │
                     └────────────────┘
```

---

## Schema Details

### Dimension Tables

| Table | Primary Key | Key Columns |
|---|---|---|
| `dimCustomer` | `CustomerKey` | `CustomerId`, `CustomerFirstName`, `CustomerLastName` |
| `dimEmployee` | `EmployeeKey` | `EmployeeId`, `EmployeeFirstName`, `EmployeeLastName` |
| `dimProducts` | `ProductKey` | `ProductId`, `ProductName` |
| `dimDate` | `DateKey` | `Date`, `Day`, `Month`, `Year`, `Quarter` |

### Fact Table

| Table | Foreign Keys | Measures |
|---|---|---|
| `factSales` | `CustomerKey`, `EmployeeKey`, `ProductKey`, `DateKey` | `ProductPrice`, `Quantity`, `OrderDate` |

---

## ETL — Stored Procedures

Each dimension and the fact table has a dedicated stored procedure that handles **both inserts and updates** in a single execution — making the pipeline fully idempotent and safe to re-run.

| Procedure | Table | Handles |
|---|---|---|
| `spUpdateDimCustomer` | `dimCustomer` | New customers + name changes |
| `spUpdateDimEmployee` | `dimEmployee` | New employees + name changes |
| `spUpdateDimProducts` | `dimProducts` | New products + name changes |
| `spLoadDimDate` | `dimDate` | Date range generation (CTE + recursion) |
| `spUpdateFactSales` | `factSales` | New transactions + price/quantity updates |

---

## ETL Pattern — Insert + Update Logic

Every stored procedure follows the same two-phase pattern:

### Phase 1 — INSERT (New Records Only)
```sql
-- LEFT JOIN back to the dimension table
-- WHERE dimension key IS NULL → record doesn't exist yet → INSERT
SELECT DISTINCT dw.*
FROM DatawarehouseSample dw
LEFT JOIN dimCustomer dc ON dw.CustomerId = dc.CustomerId
WHERE dc.CustomerId IS NULL
```

### Phase 2 — UPDATE (Changed Records Only)
```sql
-- INNER JOIN to match existing records
-- WHERE any tracked column has changed → UPDATE
WHERE dc.CustomerFirstName <> dw.CustomerFirstName
   OR dc.CustomerLastName  <> dw.CustomerLastName
```

>  This pattern ensures **no duplicates on insert** and **no stale data on update** — a core principle of reliable ETL design.

---

## Dynamic Date Dimension

The `dimDate` table is populated using a **recursive CTE** that generates one row per day across the full date range present in the source data — no hardcoded dates, no manual calendar tables:

```sql
WITH dateRange AS (
    SELECT MIN(OrderDate) AS sDate, MAX(OrderDate) AS maxDate
    FROM DatawarehouseSample
),
allDates AS (
    SELECT sDate FROM dateRange
    UNION ALL
    SELECT DATEADD(DAY, 1, sDate) FROM allDates
    WHERE sDate < (SELECT maxDate FROM dateRange)
)
INSERT INTO dimDate (Date, DateKey, Day, Month, Year, Quarter)
SELECT
    sDate,
    CAST(FORMAT(sDate, 'yyyyMMdd') AS INT) AS DateKey,
    DAY(sDate), MONTH(sDate), YEAR(sDate),
    DATEPART(QUARTER, sDate)
FROM allDates
LEFT JOIN dimDate dd ON allDates.sDate = dd.Date
WHERE dd.Date IS NULL
OPTION (MAXRECURSION 0);
```

>  `DateKey` uses `yyyyMMdd` integer format — a data warehousing standard for efficient joins and partitioning.

---

## Execution Order

Run procedures in this order to ensure referential integrity:

```sql
-- Step 1: Load dimension tables first
EXEC spUpdateDimCustomer;
EXEC spUpdateDimEmployee;
EXEC spUpdateDimProducts;
EXEC spLoadDimDate;

-- Step 2: Load fact table after all dimensions are ready
EXEC spUpdateFactSales;
```

>  Always load dimensions **before** the fact table — `factSales` holds foreign keys that reference all four dimension tables.

---

## Key SQL Concepts Used

| Concept | Applied In |
|---|---|
| **Stored Procedures** | All ETL logic encapsulated in `sp*` procedures |
| **Recursive CTE** | Dynamic date range generation in `spLoadDimDate` |
| **LEFT JOIN / WHERE NULL** | Identifying new records for insert |
| **INNER JOIN + WHERE** | Identifying changed records for update |
| **IDENTITY Primary Keys** | Auto-generated surrogate keys on all dimension tables |
| **Foreign Key Constraints** | Enforced referential integrity in `factSales` |
| **FORMAT + CAST** | Integer `DateKey` generation (`yyyyMMdd`) |
| **DISTINCT** | Deduplication during dimension inserts |
| **OPTION (MAXRECURSION 0)** | Unlimited recursion for full date range generation |

---

## Tools & Technologies

| Tool | Purpose |
|---|---|
| **SQL Server** | Database engine and warehouse host |
| **T-SQL** | All DDL, DML, and stored procedure logic |
| **Stored Procedures** | Reusable, automated ETL pipeline |
| **Star Schema Design** | Dimensional modeling for analytics |

---

##  Use Cases

This project is ideal for:

-  Learning **dimensional modeling** and star schema design from scratch
-  Understanding **ETL pipeline design** using native SQL Server features
-  Practicing **stored procedure-based automation** for data loading
-  Building a **foundation for Power BI** or other BI reporting tools
-  Portfolio projects demonstrating **production-style SQL engineering**

---

## Outcome

-  Designed and implemented a **complete star schema** from a flat staging source
-  Built a **fully automated ETL pipeline** using stored procedures
-  Implemented **insert + update logic** for all dimensions and the fact table
-  Created a **dynamic, data-driven date dimension** using recursive CTEs
-  Enforced **referential integrity** through foreign key constraints on the fact table

---

## Connect

If you found this project useful or have suggestions, feel free to open an **Issue** or submit a **Pull Request**.
