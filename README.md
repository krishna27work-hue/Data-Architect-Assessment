# Data-Architect-Assessment

# EMS End-to-End ETL (SSIS + SQL Server + Python) — Medallion + Kimball DW

This project is an end-to-end ETL pipeline for an EMS (Emergency Medical Services) CSV dataset.  
It’s built like a production pipeline: **parameterized, modular, restart-safe, and traceable**.

At a high level:
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
- Idempotency / reruns:
  - Incremental processing using `etl.watermark.LastBronzeId` (only new Bronze rows)
  - Dedupe via `RecordHash` (SHA2_256) to prevent duplicates across reruns / different RunIds

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

## Logging, Monitoring, and Error Handling

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

## Large-Data Best Practices Used
Even though the sample file is small, the design assumes scale:
- **Batching** in Silver using watermark + `TOP (@batch_size)`
- **Set-based inserts** into dims and fact (avoid row-by-row loops)
- **Indexes for operational queries** (ex: RunId lookups)
- Incremental pattern via watermark table for efficient future runs

---

## Configuration / Parameterization
The pipeline is designed to be configurable without code changes:
- SSIS parameters / variables control:
  - file path, file name, connection, RunId generation, etc.
- Python CLI parameters control:
  - connection string, `run-id`, `batch-size`, run modes (`--silver-only`, `--gold-only`, `--full-refresh`)

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

## How to Run (High Level)

### Step 1 — Ingest to Bronze (SSIS)
- Run the SSIS package to load CSV into `ems.bronze.ems_raw`
- SSIS also inserts a row into `ems.etl.run_audit` and generates the `RunId`

### Step 2 — Silver + Gold (Python)
Install deps:
```bash
pip install -r requirements.txt
