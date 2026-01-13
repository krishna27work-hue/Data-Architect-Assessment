# EMS SSIS Package – Bronze Load + Silver/Gold Trigger

## What this package does
This SSIS package loads an EMS CSV into the **Bronze** table in SQL Server, logs the run in a simple **run_audit** table (start + end), captures the Bronze row count, and then kicks off a **Python** step to build **Silver + Gold**.

---

## Control Flow (high level)
1. **Set RunId and FileName** (Script Task)  
2. **RunAuditStart** (Execute SQL Task)  
3. **Data Flow Task** (CSV → Bronze)  
4. **RunAuditEnd** (Execute SQL Task)  
5. **Run Silver+Gold (Python)** (Execute Process Task)

---

## Variables used
- `User::RunId` (String)  
  Unique run identifier (generated at runtime).

- `User::FileName` (String)  
  Captured file name for lineage/audit.

- `User::RowsBronze` (Int64 / Long)  
  Row count written after Bronze load.

- `User::PyConnStr` (String)  
  ODBC connection string passed into Python.

- `User::PyDir` (String)  
  Python/script directory location used by the last step.

- `User::PyScript` (String)  
  Python entry (module/script name) used by the last step.

> Package parameter: `$Package::p_file_path` is referenced in the Script Task (used to resolve the input file path).

---

## Connection Managers
### Flat File Connection Manager
- Points to the input CSV (currently local path like:  
  `C:\Users\Krishna Rao\Downloads\sam.csv`)
- UTF-8 (65001), Delimited
- “Column names in the first data row” enabled

### OLE DB Connection Manager (SQL Server)
- Provider: Microsoft OLE DB Driver 19 for SQL Server
- Server: `localhost\MSSQLSERVER01`
- Auth: Windows Integrated Security
- Database: `ems`

---

## Task details

### 1) Set RunId and FileName (Script Task)
Purpose:
- Generate `User::RunId`
- Set `User::FileName` based on the file path/parameter

Reads:
- `$Package::p_file_path`

Writes:
- `User::RunId`
- `User::FileName`

**Can someone see the script if they open the package?**  
Yes — if they open the `.dtsx` in Visual Studio/SSDT, they can open the Script Task and click **Edit Script** (assuming they have SSIS design tools/VSTA installed).

---

### 2) RunAuditStart (Execute SQL Task)
Purpose:
- Insert a “start” record into `ems.etl.run_audit`

Parameter mapping:
- `User::RunId` (Param 0)
- `User::FileName` (Param 1)

---

### 3) Data Flow Task (CSV → Bronze)
Pipeline:
- **Flat File Source** (reads the CSV)
- **Derived Column** (adds lineage columns)
  - `RunId = (DT_WSTR,36) @[User::RunId]`
  - `FileName = (DT_WSTR,500) @[User::FileName]`
- **Script Component** (creates `SourceRowNum`)
- **Row Count** (writes to `User::RowsBronze`)
- **OLE DB Destination** (loads into `bronze.ems_raw` using fast load)

Notes:
- Bronze has standard metadata columns like `RunId`, `FileName`, `LoadUtc`, `SourceRowNum`
- Column mappings map the CSV fields + metadata into Bronze

---

### 4) RunAuditEnd (Execute SQL Task)
Purpose:
- Update the run_audit record with end timestamp + row count

Parameter mapping:
- `User::RowsBronze` (Param 0)
- `User::RunId` (Param 1)

---

### 5) Run Silver+Gold (Python) – Execute Process Task
Purpose:
- Trigger the Python pipeline after Bronze completes

How it’s built:
- The Arguments property is expression-based and builds a command like:
  - run module/script + pass connection string + pass RunId  
  (ex: `-m src.run_pipeline --conn "<User::PyConnStr>" --run-id "<User::RunId>"`)

---

## How I run it
1. Update the Flat File Connection Manager path (or set `$Package::p_file_path`)
2. Confirm the SQL connection points to the right DB (`ems`)
3. Run the package
4. Validate:
   - Bronze rows loaded (`bronze.ems_raw`)
   - Audit start/end written (`ems.etl.run_audit`)
   - Python step ran and produced Silver/Gold outputs

---

## Quick notes
- The flat file path is currently **local** — if someone else runs it on another machine, they’ll need to update it or parameterize it properly.
- The Python step will work only if the target machine has the same Python setup / script path (or those are parameterized).

