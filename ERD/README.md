# EMS Kimball Star Schema (ERD Notes)

This ERD represents the **Gold / Data Warehouse** layer of the pipeline using **Kimball star schema** modeling.

---

## Business Process
EMS operational encounter/incident records.

## Fact Grain
**One row per EMS encounter record** (one cleaned source row from `silver.ems_clean`, deduped using `RecordHash`).

---

## Fact Table

### `dw.FactEMS_Encounter`
Central fact table that stores the measurable attributes of an EMS encounter and links to conformed dimensions.

**Measures / Flags**
- `ProviderToSceneMins`, `ProviderToDestinationMins`
- `InjuryFlg`, `NaloxoneGivenFlg`, `MedicationGivenOtherFlg`

**Lineage (Traceability)**
- `RunId`, `FileName`, `SourceRowNumber`, `RecordHash`

---

## Dimensions (Conformed)

### `dw.DimDate` (Role-Playing)
Single date dimension reused for multiple date roles in the fact:
- `IncidentDateKey`
- `UnitNotifiedDateKey`
- `ArrivedSceneDateKey`
- `ArrivedPatientDateKey`
- `LeftSceneDateKey`
- `ArrivedDestinationDateKey`

> Note: Full timestamps (`...Dttm`) are stored in the Silver layer. The DW uses **DateKey (YYYYMMDD)** derived from those timestamps.

### `dw.DimCounty`
Incident county (conformed geographic dimension).

### `dw.DimComplaint`
Chief complaint details:
- dispatch complaint
- anatomic location

### `dw.DimSymptom`
Clinical symptom context:
- primary symptom
- provider impression (primary)

### `dw.DimProvider`
Provider metadata:
- structure, service, service level  
Includes `EffectiveStart/EffectiveEnd/IsCurrent` to support SCD2 if needed (current load is Type 1 style).

### `dw.DimDisposition`
Disposition values (shared dimension used twice in the fact):
- `DispositionEDKey`
- `DispositionHospitalKey`

### `dw.DimDestinationType`
Destination category dimension.

---

## Keys & Referential Integrity

- All dimensions use **surrogate keys** (IDENTITY) as primary keys.
- The fact table stores foreign keys to each dimension and enforces referential integrity via FKs.
- Each dimension supports an `UnknownFlag` pattern (seeded `UNKNOWN` row) so facts can load even if a dimension attribute is missing/blank.

---

## Why this is a Star Schema
- A single central fact table (`FactEMS_Encounter`)
- Multiple independent dimensions surrounding it
- Dimensions do not depend on each other (only relate through the fact)
- Date dimension is role-playing, which is standard Kimball practice
