# UK Housing Data Engineering — Project Documentation

## 1. Project Overview

This project is an end-to-end UK housing data engineering pipeline built using Microsoft Azure.

The project brings together multiple public housing datasets and processes them through an automated data pipeline. Data is ingested using Azure Data Factory, stored in ADLS Gen2, transformed and validated in Azure Databricks using PySpark and Delta Lake, and accessed through Synapse Serverless SQL for reporting in Metabase.

The main focus of the project is the data engineering pipeline: ingestion, transformation, validation, standardisation and preparation of data for analytics.

---

## 2. Architecture

The overall pipeline follows this flow:

```text
Public Data Sources
        |
        v
Azure Blob Storage
        |
        | Event Trigger
        v
Azure Data Factory
        |
        v
ADLS Gen2 / Raw
        |
        v
Azure Databricks
        |
   +----+----+
   |    |    |
   v    v    v
Bronze Silver Gold
   |    |    |
   +----+----+
        |
        v
Synapse Serverless SQL
        |
        v
Metabase
```

### Azure Services

- Azure Blob Storage — source file landing area
- Azure Data Factory — event-driven ingestion and pipeline orchestration
- ADLS Gen2 — raw data storage
- Azure Databricks — PySpark transformations and validation
- Delta Lake / Unity Catalog — Gold data and table management
- Azure Key Vault — secure storage of the Databricks access token
- Synapse Serverless SQL — SQL views over the Gold data
- Metabase — dashboard and data visualisation

---

## 3. Source Data

Five public datasets are used in the project:

1. Local Authorities
2. House Prices
3. Statutory Homelessness
4. Local Authority Housing Statistics
5. English Indices of Deprivation

The datasets come from public sources including GOV.UK, the Office for National Statistics and mySociety.

Each source has a different structure, so source-specific processing is applied before the datasets are brought into the common Gold model.

### 3.1 Local Authorities

**Source:** mySociety — UK Local Authorities (past and current)

**File:** `uk_local_authorities_current.csv`

**Selected fields:**

- `local-authority-code`
- `official-name`
- `nice-name`
- `gss-code`
- `start-date`
- `end-date`
- `replaced-by`
- `nation`
- `region`
- `local-authority-type`
- `local-authority-type-name`
- `current-authority`

**Processing:**

The required local-authority reference fields were selected and `COMB` Combined Authority records were removed.

**Final rows:** 366 local authorities

### 3.2 House Prices

**Source:** Office for National Statistics — Median house prices for administrative geographies

**File:** `medianpricepaidforadministrativegeographies.xlsx`

**Sheet:** `2a`

**Selected fields:**

- `Local authority code`
- `Local authority name`
- `Year ending March 2025`

**Processing:**

The source contains England and Wales, so the data was filtered to English local authorities using the local-authority code.

**Final rows:** 296 English local authorities

### 3.3 Statutory Homelessness

**Source:** GOV.UK — Statutory homelessness in England: financial year 2024-25

**File:** `Statutory_Homelessness_Detailed_Local_Authority_Data_2024-2025_corrected-A1.csv`

**Table:** `A1`

**Selected fields:**

- `la_code`
- `la_name`
- `total_households_assessed`
- `total_duty_households`
- `prevention_duty_households`
- `relief_duty_households`

**Processing:**

The source table contains rows other than individual local authorities, so the data was filtered to English local authorities using the local-authority code condition.

**Final rows:** 296 English local authorities

### 3.4 Local Authority Housing Statistics

**Source:** GOV.UK — Local Authority Housing Statistics open data

**File:** `LAHS_open_data_1978-79_to_2024-25.csv`

**Selected fields:**

- `LAD24CD`
- `LAD24NM`
- `Year`
- `status`
- `a1a`
- `cc1a`

**Processing:**

The 2024-25 data was selected, keeping local-authority records with `Submitted` or `Saved` status.

**Final rows:** 296 local authorities

### 3.5 English Indices of Deprivation

**Source:** GOV.UK — English Indices of Deprivation 2025

**File:** `File_10_-_IoD2025_Local_Authority_District_Summaries_lower-tier_v2.xlsx`

**Selected fields:**

- `la_code`
- `la_name`
- `imd_score`
- `imd_rank`

**Processing:**

The source already covers English local authorities, so no England-only filter was applied. Three completely empty rows were removed.

**Final rows:** 296 local authorities

## 4. Data Ingestion

Source files are initially placed in an Azure Blob Storage container:

`uk-housing-source`

Azure Data Factory monitors the container using a Blob Created event trigger.

When a new file arrives, the following process takes place:

`Blob created`  
↓  
`ADF Event Trigger`  
↓  
`Filename passed to pipeline`  
↓  
`Copy Blob -> ADLS Gen2 Raw`  
↓  
`Switch based on filename`  
↓  
`Correct Databricks notebook`

The pipeline uses a single `filename` parameter.

The trigger passes:

`@triggerBody().fileName`

The filename is then used by the ADF Switch activity to determine which Databricks notebook should run.

---

## 5. Azure Data Factory

The master pipeline is:

`pl_uk_housing_ingestion`

The pipeline contains:

- Blob event trigger
- Filename parameter
- Copy activity
- Switch activity
- Databricks notebook activities

The Switch routes each source file to its corresponding Databricks notebook.

This allows the same ingestion pipeline to handle multiple datasets rather than creating a separate master pipeline for each source.

### 5.1 Event Trigger

The pipeline uses a Blob Created event trigger.

When a new file is created in the source Blob Storage container, the trigger starts the pipeline and passes the filename as a pipeline parameter.

### 5.2 Copy Activity

The Copy activity moves the source file from Azure Blob Storage into the ADLS Gen2 raw area.

### 5.3 Switch Activity

The Switch activity uses the filename to determine which Databricks notebook should run.

The pipeline currently handles the five project datasets:

- Local Authorities
- House Prices
- Homelessness
- LAHS
- IMD

---

## 6. Security

Azure Key Vault is used for the Databricks access token.

The connection follows this structure:

`ADF`  
↓  
`Managed Identity`  
↓  
`Azure Key Vault`  
↓  
`Databricks token`

**Key Vault:**

`ukhousingkeyvault`

**ADF managed identity:**

`ukhousingadf`

The Databricks linked service retrieves the token from Key Vault rather than storing the token directly in the pipeline configuration.

---

## 7. Databricks Processing

Databricks performs the main transformation and validation work using PySpark.

Unity Catalog is used to organise the project:

`uk_housing`

- `bronze`
- `silver`
- `gold`

The processing follows a Bronze → Silver → Gold structure.

---

## 8. Bronze Layer

The Bronze layer loads the source data into Delta tables and keeps the structure close to the original files.

The processing varies slightly by dataset. Required fields are selected at this stage, with limited source-level filtering applied where required.

The main Bronze steps are:

1. Read source data
2. Select required fields
3. Apply source-level filters where required
4. Write Bronze data

Examples include:

- Removing `COMB` Combined Authority records from the Local Authorities source
- Selecting English local-authority records from the Homelessness source
- Selecting the required fields from the House Prices, LAHS and IMD sources

The Bronze tables are stored in:

`uk_housing.bronze`

---

## 9. Silver Layer

The Silver layer is where the main cleaning, standardisation and validation takes place.

The processing includes:

- Checking expected columns and data types
- Checking required nulls and blank values
- Validating local-authority codes
- Checking duplicate records
- Checking code and name consistency
- Validating numeric values and ranges
- Applying dataset-specific filters
- Renaming columns
- Converting values to the required data types

The different source systems use different local-authority column names. These are standardised where appropriate to:

- `la_code`
- `la_name`

The Silver layer provides a consistent structure before data reaches the Gold layer.

The Silver tables are stored in:

`uk_housing.silver`

---

## 10. Gold Layer

The Gold layer contains the final analytics-ready tables.

The Gold processing reads from Silver, selects the fields required for the final model and updates or inserts records using Delta `MERGE`.

The Gold tables are:

`uk_housing.gold`

- `dim_local_authority`
- `fact_house_prices`
- `fact_homelessness`
- `fact_lahs`
- `fact_imd`
- `la_code_mapping`

The merge keys are:

- `dim_local_authority` — `la_code`
- `fact_house_prices` — `la_code`
- `fact_homelessness` — `la_code`
- `fact_lahs` — `la_code` + `year`
- `fact_imd` — `la_code`

Using Delta `MERGE` allows existing records to be updated and new records to be inserted when the pipeline is rerun.

For the Homelessness Gold table, the household values are converted from their source string representation to numeric values. Source values represented as `..` become null at this stage.

---

## 11. Data Quality and Validation

Validation is performed during the Databricks processing stages.

Examples include:

- Null and blank checks
- Duplicate checks
- Local-authority code validation
- Numeric type validation
- Negative-value checks
- Range checks
- Source-specific validation rules
- Local-authority code consistency checks
- Homelessness duty reconciliation

For the homelessness dataset:

`total_duty_households = prevention_duty_households + relief_duty_households`

Validation failures raise an exception and stop the pipeline before invalid data is written to the target layer.

The Gold layer uses Delta `MERGE` operations rather than simply appending records. This allows existing records to be updated and new records to be inserted without creating duplicates when processing is rerun.

---

## 12. Gold Data Summary

| Gold Table | Purpose | Records |
|---|---|---:|
| `dim_local_authority` | Local-authority reference data | 366 |
| `fact_house_prices` | Median house prices | 296 |
| `fact_homelessness` | Homelessness measures | 296 |
| `fact_lahs` | Local Authority Housing Statistics | 296 |
| `fact_imd` | Deprivation measures | 296 |
| `la_code_mapping` | Local-authority code mapping | 2 |

---

## 13. Local Authority Code Mapping

The source datasets use local-authority codes to identify each local authority. Most codes match across the datasets, but two authorities use different codes between the source datasets.

To make the datasets join consistently, the project uses a small mapping table to convert the two source codes to the standard local-authority codes used in the Gold layer.

The mappings are:

- `E08000038` → `E08000016`
- `E08000039` → `E08000019`

The mapping table is stored in:

`uk_housing.gold.la_code_mapping`

The final matching result is:

- 294 direct matches
- 2 mapped matches
- 296 total matches

---

## 14. Synapse Serverless SQL

The Gold Delta tables are accessed through Azure Synapse Serverless SQL.

**Workspace:**

`syn-uk-housing-dw`

**Database:**

`uk_housing_dw`

**Schema:**

`dw`

The following views are used:

- `dw.dim_local_authority`
- `dw.fact_house_prices`
- `dw.fact_homelessness`
- `dw.fact_lahs`
- `dw.fact_imd`

The views provide a SQL layer between the Gold Delta data and the reporting tool.

No separate serving copy of the Gold data is created.

The architecture is:

`Databricks Gold`  
↓  
`Synapse Serverless SQL`  
↓  
`SQL Views`  
↓  
`Metabase`

---

## 15. Metabase

Metabase connects to the Synapse Serverless SQL endpoint and uses the `dw` views as its data source.

**Dashboard:**

`UK Housing Dashboard`

The dashboard demonstrates that the final Gold data is structured and accessible for reporting.

Current visualisations include:

- Top 10 local authorities by median house price
- Top 10 local authorities by IMD score
- House price vs IMD score scatter plot

The dashboard is the final consumption layer of the pipeline rather than the main focus of the project.

---

## Repository Structure

```text
UK_Housing_Data_Engineering/
│
├── README.md
│
├── Architecture/
│   └── architecture-diagram.png
│
├── ADF/
│   ├── code / exported files
│   └── screenshots
│
├── Databricks/
│   ├── Bronze/
│   │   ├── code
│   │   └── screenshots
│   ├── Silver/
│   │   ├── code
│   │   └── screenshots
│   ├── Gold/
│   │   ├── code
│   │   └── screenshots
│   └── Validation/
│       ├── code
│       └── screenshots
│
├── Synapse/
│   ├── SQL code
│   └── screenshots
│
├── Metabase/
│   └── screenshots
│
├── Source_Data/
│
└── Documentation/
    └── project-documentation.md
