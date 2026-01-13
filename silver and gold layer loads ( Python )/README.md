# EMS ETL (Python) — Silver + Gold (works with SSIS Bronze)

This Python folder plugs into what I already built in SSIS.

- **SSIS** loads raw CSV → `ems.bronze.ems_raw`
- **SSIS** generates the `RunId` and writes the run header → `ems.etl.run_audit`
- **Python** handles only:
  - **Silver**: clean/type/validate + write rejects (`ems.silver.ems_clean`, `ems.silver.ems_reject`)
  - **Gold**: load dimensions then fact (`dw.*`) + optional daily summary

---

## What it expects
- SQL Server DB: `ems`
- Schemas exist: `bronze`, `silver`, `etl`, `dw`
- Bronze table already loaded by SSIS:
  - `bronze.ems_raw` (raw NVARCHAR fields + `RunId`, `FileName`, `SourceRowNum`, `BronzeId`)
- Logging tables:
  - `etl.run_audit` (overall run tracking)
  - `etl.run_step_log` (step-level tracking for Silver/Gold)
- Watermark table:
  - `etl.watermark` (tracks the last processed `BronzeId` for incremental Silver loads)
- Unknown members seeded in dims (UnknownFlag = 1) so fact loads never break referential integrity.

---

## 1) Create tables (one-time setup)
Run the DDLs to create:
- `silver.ems_clean`
- `silver.ems_reject`
- DW dims: `dw.DimDate`, `dw.DimCounty`, `dw.DimComplaint`, `dw.DimSymptom`, `dw.DimProvider`, `dw.DimDisposition`, `dw.DimDestinationType`
- DW fact: `dw.FactEMS_Encounter`
- ETL support: `etl.run_step_log`, `etl.watermark`
- Seed UNKNOWN rows in dims (UnknownFlag=1)

---

## 2) Run the pipeline
Install dependencies:
```bash
pip install -r requirements.txt


------  Run Silver + Gold:
python -m src.run_pipeline ^
  --conn "Driver={ODBC Driver 17 for SQL Server};Server=localhost\MSSQLSERVER01;Database=ems;Trusted_Connection=yes;" ^
  --run-id "YOUR_RUN_ID"

-------  Silver only:
python -m src.run_pipeline ^
  --conn "Driver={ODBC Driver 17 for SQL Server};Server=localhost\MSSQLSERVER01;Database=ems;Trusted_Connection=yes;" ^
  --run-id "YOUR_RUN_ID"


-------- Gold only
python -m src.run_pipeline --conn "<ODBC_CONN>" --run-id "YOUR_RUN_ID" --gold-only

-------- Full refresh
python -m src.run_pipeline --conn "<ODBC_CONN>" --run-id "YOUR_RUN_ID" --full-refresh


## Re-runs / idempotency

Silver:

-Incremental load using etl.watermark.LastBronzeId (only processes new Bronze rows).
-Writes invalid rows to silver.ems_reject with ErrorType + message.
-Uses RecordHash to dedupe (prevents duplicates across reruns / different RunIds).

Gold:

-Dimensions load with NOT EXISTS insert patterns (Type 1 style).
-Fact load is idempotent using RecordHash.
-Optional dw.ems_daily_summary (if table exists) is rerunnable per RunId (delete + insert).