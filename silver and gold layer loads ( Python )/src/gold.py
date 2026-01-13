# src/gold.py
import pyodbc
from .step_log import start_step, end_step

GOLD_STEP = "GOLD_LOAD"


def run_gold(conn: pyodbc.Connection, run_id: str, full_refresh: bool = False) -> None:
    # Gold = dimensional model (dims + fact) built from silver.ems_clean
    step_log_id = start_step(conn, run_id, GOLD_STEP)
    rows_in = 0
    rows_out = 0

    try:
        cur = conn.cursor()

        # --------------------------
        # Full refresh (dev/testing)
        # --------------------------
        if full_refresh:
            # fact first because of FK constraints
            cur.execute("DELETE FROM dw.FactEMS_Encounter;")

            # keep UNKNOWN rows in dims (UnknownFlag=1), delete only business rows
            cur.execute("DELETE FROM dw.DimComplaint WHERE UnknownFlag = 0;")
            cur.execute("DELETE FROM dw.DimSymptom WHERE UnknownFlag = 0;")
            cur.execute("DELETE FROM dw.DimProvider WHERE UnknownFlag = 0;")
            cur.execute("DELETE FROM dw.DimCounty WHERE UnknownFlag = 0;")
            cur.execute("DELETE FROM dw.DimDisposition WHERE UnknownFlag = 0;")
            cur.execute("DELETE FROM dw.DimDestinationType WHERE UnknownFlag = 0;")

            # date dim can be rebuilt anytime
            cur.execute("DELETE FROM dw.DimDate;")

            # optional summary reset (only if the table exists)
            cur.execute("""
                IF OBJECT_ID('dw.ems_daily_summary','U') IS NOT NULL
                    DELETE FROM dw.ems_daily_summary;
            """)
            conn.commit()

        # --------------------------
        # basic counts (for step log)
        # --------------------------
        cur.execute("SELECT COUNT(1) FROM silver.ems_clean;")
        rows_in = int(cur.fetchone()[0])

        # --------------------------
        # DimDate (build from all datetime columns)
        # --------------------------
        cur.execute("""
        ;WITH d AS (
            SELECT CAST(IncidentDttm AS date) AS dt FROM silver.ems_clean WHERE IncidentDttm IS NOT NULL
            UNION
            SELECT CAST(UnitNotifiedByDispatchDttm AS date) FROM silver.ems_clean WHERE UnitNotifiedByDispatchDttm IS NOT NULL
            UNION
            SELECT CAST(UnitArrivedOnSceneDttm AS date) FROM silver.ems_clean WHERE UnitArrivedOnSceneDttm IS NOT NULL
            UNION
            SELECT CAST(UnitArrivedToPatientDttm AS date) FROM silver.ems_clean WHERE UnitArrivedToPatientDttm IS NOT NULL
            UNION
            SELECT CAST(UnitLeftSceneDttm AS date) FROM silver.ems_clean WHERE UnitLeftSceneDttm IS NOT NULL
            UNION
            SELECT CAST(PatientArrivedDestinationDttm AS date) FROM silver.ems_clean WHERE PatientArrivedDestinationDttm IS NOT NULL
        )
        INSERT INTO dw.DimDate (DateKey, FullDate, [Year], [Quarter], [Month], [Day], DayOfWeek, DayName, MonthName, IsWeekend)
        SELECT
            CONVERT(int, CONVERT(char(8), dt, 112)) AS DateKey,
            dt AS FullDate,
            DATEPART(year, dt) AS [Year],
            DATEPART(quarter, dt) AS [Quarter],
            DATEPART(month, dt) AS [Month],
            DATEPART(day, dt) AS [Day],
            DATEPART(isowk, dt) * 0 + DATEPART(weekday, dt) AS DayOfWeek,  -- simple weekday number
            DATENAME(weekday, dt) AS DayName,
            DATENAME(month, dt) AS MonthName,
            CASE WHEN DATENAME(weekday, dt) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS IsWeekend
        FROM (SELECT DISTINCT dt FROM d) x
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.DimDate dd
            WHERE dd.DateKey = CONVERT(int, CONVERT(char(8), x.dt, 112))
        );
        """)
        conn.commit()

        # --------------------------
        # cache UNKNOWN keys once
        # --------------------------
        cur.execute("SELECT CountyKey FROM dw.DimCounty WHERE UnknownFlag=1;")
        unk_county = int(cur.fetchone()[0])

        cur.execute("SELECT ComplaintKey FROM dw.DimComplaint WHERE UnknownFlag=1;")
        unk_complaint = int(cur.fetchone()[0])

        cur.execute("SELECT SymptomKey FROM dw.DimSymptom WHERE UnknownFlag=1;")
        unk_symptom = int(cur.fetchone()[0])

        cur.execute("SELECT ProviderKey FROM dw.DimProvider WHERE UnknownFlag=1;")
        unk_provider = int(cur.fetchone()[0])

        cur.execute("SELECT DispositionKey FROM dw.DimDisposition WHERE UnknownFlag=1;")
        unk_disposition = int(cur.fetchone()[0])

        cur.execute("SELECT DestinationTypeKey FROM dw.DimDestinationType WHERE UnknownFlag=1;")
        unk_desttype = int(cur.fetchone()[0])

        # --------------------------
        # load dims (type 1 style)
        # --------------------------
        cur.execute("""
        INSERT INTO dw.DimCounty (CountyName)
        SELECT DISTINCT s.IncidentCounty
        FROM silver.ems_clean s
        WHERE s.IncidentCounty IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dw.DimCounty d WHERE d.CountyName = s.IncidentCounty
          );
        """)

        cur.execute("""
        INSERT INTO dw.DimComplaint (ChiefComplaintDispatch, ChiefComplaintAnatomicLoc)
        SELECT DISTINCT s.ChiefComplaintDispatch, s.ChiefComplaintAnatomicLoc
        FROM silver.ems_clean s
        WHERE (s.ChiefComplaintDispatch IS NOT NULL OR s.ChiefComplaintAnatomicLoc IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1
              FROM dw.DimComplaint d
              WHERE ISNULL(d.ChiefComplaintDispatch,'') = ISNULL(s.ChiefComplaintDispatch,'')
                AND ISNULL(d.ChiefComplaintAnatomicLoc,'') = ISNULL(s.ChiefComplaintAnatomicLoc,'')
          );
        """)

        cur.execute("""
        INSERT INTO dw.DimSymptom (PrimarySymptom, ProviderImpressionPrimary)
        SELECT DISTINCT s.PrimarySymptom, s.ProviderImpressionPrimary
        FROM silver.ems_clean s
        WHERE (s.PrimarySymptom IS NOT NULL OR s.ProviderImpressionPrimary IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1
              FROM dw.DimSymptom d
              WHERE ISNULL(d.PrimarySymptom,'') = ISNULL(s.PrimarySymptom,'')
                AND ISNULL(d.ProviderImpressionPrimary,'') = ISNULL(s.ProviderImpressionPrimary,'')
          );
        """)

        # provider dim is structured for SCD2 later, but for this assignment we load current rows only (type 1 style)
        cur.execute("""
        INSERT INTO dw.DimProvider (ProviderTypeStructure, ProviderTypeService, ProviderTypeServiceLevel)
        SELECT DISTINCT s.ProviderTypeStructure, s.ProviderTypeService, s.ProviderTypeServiceLevel
        FROM silver.ems_clean s
        WHERE (s.ProviderTypeStructure IS NOT NULL OR s.ProviderTypeService IS NOT NULL OR s.ProviderTypeServiceLevel IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1
              FROM dw.DimProvider d
              WHERE d.IsCurrent = 1
                AND ISNULL(d.ProviderTypeStructure,'') = ISNULL(s.ProviderTypeStructure,'')
                AND ISNULL(d.ProviderTypeService,'') = ISNULL(s.ProviderTypeService,'')
                AND ISNULL(d.ProviderTypeServiceLevel,'') = ISNULL(s.ProviderTypeServiceLevel,'')
          );
        """)

        # disposition comes from two columns (ED and Hospital) so union them into one dim
        cur.execute("""
        INSERT INTO dw.DimDisposition (DispositionName)
        SELECT DISTINCT x.DispositionName
        FROM (
            SELECT s.DispositionED AS DispositionName
            FROM silver.ems_clean s
            WHERE s.DispositionED IS NOT NULL
            UNION
            SELECT s.DispositionHospital
            FROM silver.ems_clean s
            WHERE s.DispositionHospital IS NOT NULL
        ) x
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.DimDisposition d WHERE d.DispositionName = x.DispositionName
        );
        """)

        cur.execute("""
        INSERT INTO dw.DimDestinationType (DestinationTypeName)
        SELECT DISTINCT s.DestinationType
        FROM silver.ems_clean s
        WHERE s.DestinationType IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dw.DimDestinationType d WHERE d.DestinationTypeName = s.DestinationType
          );
        """)
        conn.commit()

        # --------------------------
        # load fact (dedupe by RecordHash)
        # --------------------------
        cur.execute("""
        INSERT INTO dw.FactEMS_Encounter (
            IncidentDateKey, UnitNotifiedDateKey, ArrivedSceneDateKey, ArrivedPatientDateKey, LeftSceneDateKey, ArrivedDestinationDateKey,
            CountyKey, ComplaintKey, SymptomKey, ProviderKey, DispositionEDKey, DispositionHospitalKey, DestinationTypeKey,
            ProviderToSceneMins, ProviderToDestinationMins, InjuryFlg, NaloxoneGivenFlg, MedicationGivenOtherFlg,
            RunId, FileName, SourceRowNumber, RecordHash
        )
        SELECT
            CASE WHEN s.IncidentDttm IS NULL THEN NULL ELSE CONVERT(int, CONVERT(char(8), CAST(s.IncidentDttm AS date), 112)) END,
            CASE WHEN s.UnitNotifiedByDispatchDttm IS NULL THEN NULL ELSE CONVERT(int, CONVERT(char(8), CAST(s.UnitNotifiedByDispatchDttm AS date), 112)) END,
            CASE WHEN s.UnitArrivedOnSceneDttm IS NULL THEN NULL ELSE CONVERT(int, CONVERT(char(8), CAST(s.UnitArrivedOnSceneDttm AS date), 112)) END,
            CASE WHEN s.UnitArrivedToPatientDttm IS NULL THEN NULL ELSE CONVERT(int, CONVERT(char(8), CAST(s.UnitArrivedToPatientDttm AS date), 112)) END,
            CASE WHEN s.UnitLeftSceneDttm IS NULL THEN NULL ELSE CONVERT(int, CONVERT(char(8), CAST(s.UnitLeftSceneDttm AS date), 112)) END,
            CASE WHEN s.PatientArrivedDestinationDttm IS NULL THEN NULL ELSE CONVERT(int, CONVERT(char(8), CAST(s.PatientArrivedDestinationDttm AS date), 112)) END,

            ISNULL(c.CountyKey, ?) AS CountyKey,
            ISNULL(cc.ComplaintKey, ?) AS ComplaintKey,
            ISNULL(sm.SymptomKey, ?) AS SymptomKey,
            ISNULL(p.ProviderKey, ?) AS ProviderKey,
            ISNULL(ded.DispositionKey, ?) AS DispositionEDKey,
            ISNULL(dh.DispositionKey, ?) AS DispositionHospitalKey,
            ISNULL(dt.DestinationTypeKey, ?) AS DestinationTypeKey,

            s.ProviderToSceneMins,
            s.ProviderToDestinationMins,
            s.InjuryFlg,
            s.NaloxoneGivenFlg,
            s.MedicationGivenOtherFlg,

            s.RunId,
            s.FileName,
            s.SourceRowNum AS SourceRowNumber,  -- fact DDL uses SourceRowNumber
            s.RecordHash
        FROM silver.ems_clean s
        LEFT JOIN dw.DimCounty c
            ON c.CountyName = s.IncidentCounty
        LEFT JOIN dw.DimComplaint cc
            ON ISNULL(cc.ChiefComplaintDispatch,'') = ISNULL(s.ChiefComplaintDispatch,'')
           AND ISNULL(cc.ChiefComplaintAnatomicLoc,'') = ISNULL(s.ChiefComplaintAnatomicLoc,'')
        LEFT JOIN dw.DimSymptom sm
            ON ISNULL(sm.PrimarySymptom,'') = ISNULL(s.PrimarySymptom,'')
           AND ISNULL(sm.ProviderImpressionPrimary,'') = ISNULL(s.ProviderImpressionPrimary,'')
        LEFT JOIN dw.DimProvider p
            ON p.IsCurrent = 1
           AND ISNULL(p.ProviderTypeStructure,'') = ISNULL(s.ProviderTypeStructure,'')
           AND ISNULL(p.ProviderTypeService,'') = ISNULL(s.ProviderTypeService,'')
           AND ISNULL(p.ProviderTypeServiceLevel,'') = ISNULL(s.ProviderTypeServiceLevel,'')
        LEFT JOIN dw.DimDisposition ded
            ON ded.DispositionName = s.DispositionED
        LEFT JOIN dw.DimDisposition dh
            ON dh.DispositionName = s.DispositionHospital
        LEFT JOIN dw.DimDestinationType dt
            ON dt.DestinationTypeName = s.DestinationType
        WHERE NOT EXISTS (
            SELECT 1
            FROM dw.FactEMS_Encounter f
            WHERE f.RecordHash = s.RecordHash
        );
        """,
        unk_county, unk_complaint, unk_symptom, unk_provider, unk_disposition, unk_disposition, unk_desttype)

        try:
            rows_out = max(cur.rowcount or 0, 0)
        except Exception:
            rows_out = 0

        conn.commit()

        # --------------------------
        # optional daily summary 
        # --------------------------
        cur.execute("""
        IF OBJECT_ID('dw.ems_daily_summary','U') IS NOT NULL
        BEGIN
            -- rerunnable for same run_id
            DELETE FROM dw.ems_daily_summary WHERE RunId = ?;

            INSERT INTO dw.ems_daily_summary (RunId, IncidentDate, IncidentCounty, TotalIncidents, InjuryYes, NaloxoneYes)
            SELECT
                ?,
                CAST(IncidentDttm AS date),
                IncidentCounty,
                COUNT_BIG(1),
                SUM(CASE WHEN InjuryFlg = 'Y' THEN 1 ELSE 0 END),
                SUM(CASE WHEN NaloxoneGivenFlg = 'Y' THEN 1 ELSE 0 END)
            FROM silver.ems_clean
            WHERE RunId = ?
              AND IncidentDttm IS NOT NULL
            GROUP BY CAST(IncidentDttm AS date), IncidentCounty;
        END
        """, run_id, run_id, run_id)
        conn.commit()

        end_step(conn, step_log_id, "SUCCESS", rows_in=rows_in, rows_out=rows_out, rows_reject=0)

    except Exception as ex:
        end_step(conn, step_log_id, "FAILED", rows_in=rows_in, rows_out=rows_out, rows_reject=0, error_message=str(ex))
        raise
