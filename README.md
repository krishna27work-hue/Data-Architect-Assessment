# Data-Architect-Assessment

# EMS End-to-End ETL (SSIS + SQL Server + Python) — Medallion + Kimball DW

This project is an end-to-end ETL pipeline for an EMS (Emergency Medical Services) CSV dataset.  
It’s built like a production pipeline: **parameterized, modular, restart-safe, and traceable
The pipeline ingests raw files, preserves traceability, applies data quality + transformations, and publishes a Kimball-style dimensional warehouse that’s rerunnable and scalable.

**Tech stack**
- **SQL Server** (staging + Silver + DW)
- **SSIS** handles ingestion of raw CSV → **Bronze** (staging / traceability)
- **Python + set-based SQL** handles **Silver** (clean + validate) and **Gold** (Kimball dimensional model)

---

## Architecture Overview (Medallion → Kimball)

### 1) Bronze (Raw / Traceability)
**Goal:** preserve the source exactly as received so we can audit, replay, and troubleshoot.

- Table: `ems.bronze.ems_raw`
- All source fields stored as **NVARCHAR** (no casting in Bronze)
- Append-only design (multiple runs/files can coexist)
- Lineage columns included:
  - `RunId` (ties to `etl.run_audit`)
  - `FileName`, `LoadUtc`, `SourceRowNum`

**Ownership:** SSIS loads Bronze using fast load patterns from the CSV.

---

### 2) Silver (Clean / Conformed Row-Level Dataset)
**Goal:** produce a consistent, typed, analytics-ready dataset + track rejects.

- Tables:
  - `ems.silver.ems_clean` (typed + standardized)
  - `ems.silver.ems_reject` (bad records with reason)
- Key logic:
  - Type conversions using `TRY_CONVERT`
  - Trimming/normalization (`LTRIM/RTRIM`, null-if-blank)
  - Domain validation for flags (Y/N normalization)
  - Reject routing with `ErrorType` + `ErrorMessage`
  - Required field checks (ex: missing/invalid incident datetime, missing county)
- Idempotency / reruns:
  - Incremental processing using `etl.watermark.LastBronzeId` (only new Bronze rows)
  - Dedupe via `RecordHash` (SHA2_256) to prevent duplicates across reruns / different RunIds
- Derived fields:
  - `IncidentDate` persisted from `IncidentDttm`
  - `RecordHash` computed as SHA2_256 fingerprint of normalized content (see Idempotency)


**Ownership:** Python runs Silver logic in batches (default **50k**) and logs step results.

---

### 3) Gold (Dimensional Warehouse / Kimball)
**Goal:** publish cleaned EMS encounters into a star schema for BI and reporting.

- Dimensions:
  - `dw.DimDate`
  - `dw.DimCounty`
  - `dw.DimComplaint`
  - `dw.DimSymptom`
  - `dw.DimProvider` *(structured to support SCD2 if needed; current implementation is Type 1 insert-only)*
  - `dw.DimDisposition`
  - `dw.DimDestinationType`
- Fact:
  - `dw.FactEMS_Encounter`

**Fact grain (business process):**
- **One row per EMS encounter record** (per source row in the cleaned Silver dataset)

**Conformed/Reusable dimensions:**
- County, Date, Provider, Disposition, Destination Type, Complaint, Symptom

**Unknown/default member handling:**
- Each dimension has an `UnknownFlag` and is seeded with an `UNKNOWN` record
- Fact loads use `ISNULL(dimKey, UnknownKey)` so referential integrity never breaks

**Idempotency:**
- Fact load uses `RecordHash` as a “row fingerprint” to avoid re-inserting the same encounter

**Optional aggregate:**
- If present, `dw.ems_daily_summary` is populated per `RunId` (daily county-level counts + simple KPIs)

---
## 4) Dimensional Modeling Decisions (Kimball)

### Surrogate keys + unknown members
- Each dimension uses a surrogate key (IDENTITY)
- Dimensions include `UnknownFlag`
- UNKNOWN rows are **seeded** once and preserved (UnknownFlag=1)
- Fact load uses `ISNULL(dimKey, UnknownKey)` so referential integrity never breaks

### SCD approach (Type 1 vs Type 2)
- Most dimensions are treated as **Type 1 (insert-only / no updates)** because:
  - dataset does not provide a stable natural key or effective-dated history for changes
  - the goal is to publish clean reporting dimensions quickly for the assessment
- `dw.DimProvider` is **structured to support SCD2** (`EffectiveStart`, `EffectiveEnd`, `IsCurrent`)
  - current implementation loads “current” values only (Type 1 behavior)
  - SCD2 can be enabled later if change capture is introduced

---
## 5) Idempotency & Incremental Strategy (Production-minded)

### No business key provided → RecordHash strategy
The source CSV does not include a stable encounter/business ID.  
To support reruns and avoid duplicate facts, I used:
- `RecordHash` = SHA2_256 of a normalized concatenation of key attributes
- Silver and Gold both use `RecordHash` to avoid re-inserting the same logical row

### Incremental Silver loads (Watermark)
- `etl.watermark.LastBronzeId` tracks last processed `BronzeId`
- Silver reads Bronze in batches using `TOP (@batch_size)` and `BronzeId > LastBronzeId`
- After each batch, watermark is advanced safely

This pattern scales well for large files and supports restartability.


## 6) Logging, Monitoring, and Error Handling

### Run-level logging
- `ems.etl.run_audit`
  - `RunId`, `StartedUtc`, `EndedUtc`, `Status`
  - high-level counts + error message

### Step-level logging
- `ems.etl.run_step_log`
  - one row per step per run (`SILVER_LOAD`, `GOLD_LOAD`)
  - start/end timestamps, row counts, error details

### Reject handling
- Invalid rows are written to `ems.silver.ems_reject`
- Clean rows are written to `ems.silver.ems_clean`

This makes it easy to answer:
- what failed, where it failed, why it failed
- how many rows were processed vs rejected
- whether a rerun is safe

---

## 7) Large-Data Best Practices Used
Even though the sample file is small, the design assumes scale:
- **Batching** in Silver using watermark + `TOP (@batch_size)`
- **Set-based inserts** into dims and fact (no row-by-row loops)
- **Indexes for operational queries** (ex: RunId lookups)
- Incremental pattern via watermark table for efficient future runs

---

## 8) Configuration / Parameterization
The pipeline is designed to be configurable without code changes:
- SSIS parameters / variables control:
  - file path, file name, connection, RunId generation, etc.
- Python CLI parameters control:
  - connection string, `run-id`, `batch-size`, run modes (`--silver-only`, `--gold-only`, `--full-refresh`)

This keeps the design modular:
- Extract/Stage (SSIS)
- Transform/Clean (Silver)
- Publish/Model (Gold)

---

## Assumptions / Design Notes
- **Target platform:** SQL Server (tables + set-based transformations).
- **No business key provided:** the source does not contain a stable encounter/business ID.  
  - For idempotency/deduping I used **`RecordHash` (SHA2_256)** computed from the normalized row content.
  - This lets the pipeline avoid re-loading the same logical record across reruns / different RunIds.
- **SCD approach:** most dims are handled Type 1 (insert-only + no updates) because the source is operational and does not provide clear change history.  
  - `dw.DimProvider` is structured with `EffectiveStart/EffectiveEnd/IsCurrent` to support **SCD2** if required later.
- **Unknown members:** dims are seeded with `UNKNOWN` (UnknownFlag=1) so fact loads don’t fail when a dimension attribute is missing/blank.
- **Incremental pattern:** Silver uses `etl.watermark.LastBronzeId` to process only new rows from Bronze.
- **Operational controls**
  - Add retries around transient SQL failures and short backoff for connectivity issues.

- **Bigger compute (Spark / distributed processing)**
  - If Bronze grows to tens/hundreds of GB per day (or we start getting many files per day), the Silver step is the first place I’d move to **Spark**.
  - Practical approach:
    - Land raw files in **S3 (or ADLS)**, read with Spark (CSV → DataFrame), apply cleaning/standardization rules, and write Silver as **Parquet/Delta**.
    - Keep the same medallion layout: `bronze/` (raw), `silver/` (clean), `gold/` (curated).
    - Use Spark to generate the same `RecordHash` and reject outputs (bad rows → `silver_reject/`).
  - Why Spark helps:
    - Handles wide CSV + large volumes without single-machine memory limits
    - Parallelizes expensive operations (parsing datetimes, normalizing text, generating hashes)
    - More efficient storage formats (Parquet/Delta) reduce downstream scan costs

---

## File Ingestion Approach (Current vs Practical Production Plan)

### What I did for the assessment (time-boxed)
- Used a **local CSV path** for SSIS to ingest into `ems.bronze.ems_raw`.

### Practical production approach
If this were a real pipeline, I’d set it up like this:
1. **Source → SFTP drop** (vendor/client drops daily files)
2. A small transfer job moves files from **SFTP → S3 landing bucket**  
   (for durability, retry, and keeping a clean “landing zone”)
3. SSIS picks up files from S3 (or a mounted/accessible location) and loads **Bronze** using fast load  
   - This can be implemented by:
     - pulling the file from S3 to a staging folder as part of the job, or
     - reading directly if the environment supports it (common in enterprise setups)
4. Silver/Gold runs after Bronze completes (same `RunId` across steps for end-to-end lineage)

This keeps ingestion operationally strong:
- easy replays (S3 landing keeps originals)
- clean separation of responsibilities (landing vs processing)
- safer incremental processing and backfills

---

Thank You,
Krishna Rao Korukanti
