# src/silver.py
import pyodbc
from .step_log import start_step, end_step
from .watermark import get_last_bronze_id, set_last_bronze_id

SILVER_STEP = "SILVER_LOAD"
DEFAULT_BATCH_SIZE = 50000


def run_silver(
    conn: pyodbc.Connection,
    run_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    full_refresh: bool = False
) -> None:
    """
    Silver = clean/typed version of bronze + a reject table.
    - Incremental: uses watermark (LastBronzeId) so we don't rescan all bronze every run
    - Rejects: bad rows go to silver.ems_reject with a simple error type
    - Dedupe: RecordHash prevents duplicates across reruns / different RunIds
    """
    step_log_id = start_step(conn, run_id, SILVER_STEP)

    rows_in_total = 0
    rows_out_total = 0
    rows_reject_total = 0

    try:
        cur = conn.cursor()

        if full_refresh:
            # full reset (handy during dev). also reset watermark back to 0
            cur.execute("TRUNCATE TABLE silver.ems_reject;")
            cur.execute("TRUNCATE TABLE silver.ems_clean;")
            conn.commit()
            set_last_bronze_id(conn, 0)

        last_bronze_id = get_last_bronze_id(conn)

        # find current max bronze id so we know when to stop batching
        cur.execute("SELECT ISNULL(MAX(BronzeId), 0) FROM bronze.ems_raw;")
        max_bronze_id = int(cur.fetchone()[0])

        while last_bronze_id < max_bronze_id:
            # pull the next chunk from bronze by BronzeId (simple incremental pattern)
            # -----------------------
            # 1) Write rejects
            # -----------------------
            reject_sql = """
            ;WITH batch AS (
                SELECT TOP (?) *
                FROM bronze.ems_raw
                WHERE BronzeId > ?
                ORDER BY BronzeId
            ),
            validated AS (
                SELECT
                    b.RunId,
                    b.FileName,
                    b.SourceRowNum,
                    b.BronzeId,

                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.INCIDENT_DT)), '')) AS IncidentDttm,

                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_NOTIFIED_BY_DISPATCH_DT)), '')) AS UnitNotifiedByDispatchDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_ARRIVED_ON_SCENE_DT)), '')) AS UnitArrivedOnSceneDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_ARRIVED_TO_PATIENT_DT)), '')) AS UnitArrivedToPatientDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_LEFT_SCENE_DT)), '')) AS UnitLeftSceneDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.PATIENT_ARRIVED_DESTINATION_DT)), '')) AS PatientArrivedDestinationDttm,

                    TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(b.PROVIDER_TO_SCENE_MINS)), '')) AS ProviderToSceneMins,
                    TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(b.PROVIDER_TO_DESTINATION_MINS)), '')) AS ProviderToDestinationMins,

                    NULLIF(LTRIM(RTRIM(b.INCIDENT_COUNTY)), '') AS IncidentCounty,

                    NULLIF(LTRIM(RTRIM(b.CHIEF_COMPLAINT_DISPATCH)), '') AS ChiefComplaintDispatch,
                    NULLIF(LTRIM(RTRIM(b.CHIEF_COMPLAINT_ANATOMIC_LOC)), '') AS ChiefComplaintAnatomicLoc,
                    NULLIF(LTRIM(RTRIM(b.PRIMARY_SYMPTOM)), '') AS PrimarySymptom,
                    NULLIF(LTRIM(RTRIM(b.PROVIDER_IMPRESSION_PRIMARY)), '') AS ProviderImpressionPrimary,

                    NULLIF(LTRIM(RTRIM(b.DISPOSITION_ED)), '') AS DispositionED,
                    NULLIF(LTRIM(RTRIM(b.DISPOSITION_HOSPITAL)), '') AS DispositionHospital,
                    NULLIF(LTRIM(RTRIM(b.DESTINATION_TYPE)), '') AS DestinationType,

                    NULLIF(LTRIM(RTRIM(b.PROVIDER_TYPE_STRUCTURE)), '') AS ProviderTypeStructure,
                    NULLIF(LTRIM(RTRIM(b.PROVIDER_TYPE_SERVICE)), '') AS ProviderTypeService,
                    NULLIF(LTRIM(RTRIM(b.PROVIDER_TYPE_SERVICE_LEVEL)), '') AS ProviderTypeServiceLevel,

                    -- normalize flags into Y/N; anything weird becomes 'X' so it gets rejected
                    CASE WHEN UPPER(LTRIM(RTRIM(b.INJURY_FLG))) IN ('Y','YES','1','TRUE','T') THEN 'Y'
                         WHEN UPPER(LTRIM(RTRIM(b.INJURY_FLG))) IN ('N','NO','0','FALSE','F') THEN 'N'
                         WHEN NULLIF(LTRIM(RTRIM(b.INJURY_FLG)), '') IS NULL THEN NULL
                         ELSE 'X' END AS InjuryFlg,

                    CASE WHEN UPPER(LTRIM(RTRIM(b.NALOXONE_GIVEN_FLG))) IN ('Y','YES','1','TRUE','T') THEN 'Y'
                         WHEN UPPER(LTRIM(RTRIM(b.NALOXONE_GIVEN_FLG))) IN ('N','NO','0','FALSE','F') THEN 'N'
                         WHEN NULLIF(LTRIM(RTRIM(b.NALOXONE_GIVEN_FLG)), '') IS NULL THEN NULL
                         ELSE 'X' END AS NaloxoneGivenFlg,

                    CASE WHEN UPPER(LTRIM(RTRIM(b.MEDICATION_GIVEN_OTHER_FLG))) IN ('Y','YES','1','TRUE','T') THEN 'Y'
                         WHEN UPPER(LTRIM(RTRIM(b.MEDICATION_GIVEN_OTHER_FLG))) IN ('N','NO','0','FALSE','F') THEN 'N'
                         WHEN NULLIF(LTRIM(RTRIM(b.MEDICATION_GIVEN_OTHER_FLG)), '') IS NULL THEN NULL
                         ELSE 'X' END AS MedicationGivenOtherFlg
                FROM batch b
            ),
            rejected AS (
                SELECT
                    v.RunId, v.FileName, v.SourceRowNum, v.BronzeId,
                    CASE
                        WHEN v.IncidentDttm IS NULL THEN 'INVALID_INCIDENT_DT'
                        WHEN v.IncidentCounty IS NULL THEN 'MISSING_COUNTY'
                        WHEN v.InjuryFlg = 'X' THEN 'INVALID_INJURY_FLG'
                        WHEN v.NaloxoneGivenFlg = 'X' THEN 'INVALID_NALOXONE_FLG'
                        WHEN v.MedicationGivenOtherFlg = 'X' THEN 'INVALID_MED_GIVEN_FLG'
                        ELSE NULL
                    END AS ErrorType
                FROM validated v
                WHERE
                    v.IncidentDttm IS NULL
                    OR v.IncidentCounty IS NULL
                    OR v.InjuryFlg = 'X'
                    OR v.NaloxoneGivenFlg = 'X'
                    OR v.MedicationGivenOtherFlg = 'X'
            )
            INSERT INTO silver.ems_reject (RunId, FileName, SourceRowNum, ErrorType, ErrorMessage)
            SELECT
                r.RunId,
                r.FileName,
                r.SourceRowNum,
                r.ErrorType,
                CONCAT('Row rejected in silver validation. BronzeId=', r.BronzeId)
            FROM rejected r
            WHERE NOT EXISTS (
                -- keep it rerunnable (avoid duplicate reject rows)
                SELECT 1
                FROM silver.ems_reject x
                WHERE x.RunId = r.RunId AND x.SourceRowNum = r.SourceRowNum
            );
            """

            # -----------------------
            # 2) Load clean rows
            # -----------------------
            clean_sql = """
            ;WITH batch AS (
                SELECT TOP (?) *
                FROM bronze.ems_raw
                WHERE BronzeId > ?
                ORDER BY BronzeId
            ),
            rejected_keys AS (
                -- anything already rejected for this run/rownum should not go to clean
                SELECT RunId, SourceRowNum
                FROM silver.ems_reject
            ),
            typed AS (
                SELECT
                    b.RunId,
                    b.FileName,
                    b.SourceRowNum,

                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.INCIDENT_DT)), '')) AS IncidentDttm,

                    NULLIF(LTRIM(RTRIM(b.INCIDENT_COUNTY)), '') AS IncidentCounty,

                    NULLIF(LTRIM(RTRIM(b.CHIEF_COMPLAINT_DISPATCH)), '') AS ChiefComplaintDispatch,
                    NULLIF(LTRIM(RTRIM(b.CHIEF_COMPLAINT_ANATOMIC_LOC)), '') AS ChiefComplaintAnatomicLoc,
                    NULLIF(LTRIM(RTRIM(b.PRIMARY_SYMPTOM)), '') AS PrimarySymptom,
                    NULLIF(LTRIM(RTRIM(b.PROVIDER_IMPRESSION_PRIMARY)), '') AS ProviderImpressionPrimary,

                    NULLIF(LTRIM(RTRIM(b.DISPOSITION_ED)), '') AS DispositionED,
                    NULLIF(LTRIM(RTRIM(b.DISPOSITION_HOSPITAL)), '') AS DispositionHospital,
                    NULLIF(LTRIM(RTRIM(b.DESTINATION_TYPE)), '') AS DestinationType,

                    NULLIF(LTRIM(RTRIM(b.PROVIDER_TYPE_STRUCTURE)), '') AS ProviderTypeStructure,
                    NULLIF(LTRIM(RTRIM(b.PROVIDER_TYPE_SERVICE)), '') AS ProviderTypeService,
                    NULLIF(LTRIM(RTRIM(b.PROVIDER_TYPE_SERVICE_LEVEL)), '') AS ProviderTypeServiceLevel,

                    TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(b.PROVIDER_TO_SCENE_MINS)), '')) AS ProviderToSceneMins,
                    TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(b.PROVIDER_TO_DESTINATION_MINS)), '')) AS ProviderToDestinationMins,

                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_NOTIFIED_BY_DISPATCH_DT)), '')) AS UnitNotifiedByDispatchDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_ARRIVED_ON_SCENE_DT)), '')) AS UnitArrivedOnSceneDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_ARRIVED_TO_PATIENT_DT)), '')) AS UnitArrivedToPatientDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.UNIT_LEFT_SCENE_DT)), '')) AS UnitLeftSceneDttm,
                    TRY_CONVERT(DATETIME2(0), NULLIF(LTRIM(RTRIM(b.PATIENT_ARRIVED_DESTINATION_DT)), '')) AS PatientArrivedDestinationDttm,

                    -- here we keep flags as Y/N/NULL only (rejects already handled above)
                    CASE WHEN UPPER(LTRIM(RTRIM(b.INJURY_FLG))) IN ('Y','YES','1','TRUE','T') THEN 'Y'
                         WHEN UPPER(LTRIM(RTRIM(b.INJURY_FLG))) IN ('N','NO','0','FALSE','F') THEN 'N'
                         ELSE NULL END AS InjuryFlg,

                    CASE WHEN UPPER(LTRIM(RTRIM(b.NALOXONE_GIVEN_FLG))) IN ('Y','YES','1','TRUE','T') THEN 'Y'
                         WHEN UPPER(LTRIM(RTRIM(b.NALOXONE_GIVEN_FLG))) IN ('N','NO','0','FALSE','F') THEN 'N'
                         ELSE NULL END AS NaloxoneGivenFlg,

                    CASE WHEN UPPER(LTRIM(RTRIM(b.MEDICATION_GIVEN_OTHER_FLG))) IN ('Y','YES','1','TRUE','T') THEN 'Y'
                         WHEN UPPER(LTRIM(RTRIM(b.MEDICATION_GIVEN_OTHER_FLG))) IN ('N','NO','0','FALSE','F') THEN 'N'
                         ELSE NULL END AS MedicationGivenOtherFlg,

                    -- record-level hash so we can dedupe across reruns / different RunIds
                    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT(
                        ISNULL(UPPER(LTRIM(RTRIM(b.INCIDENT_DT))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.INCIDENT_COUNTY))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.CHIEF_COMPLAINT_DISPATCH))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.CHIEF_COMPLAINT_ANATOMIC_LOC))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PRIMARY_SYMPTOM))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PROVIDER_IMPRESSION_PRIMARY))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.DISPOSITION_ED))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.DISPOSITION_HOSPITAL))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.DESTINATION_TYPE))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PROVIDER_TYPE_STRUCTURE))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PROVIDER_TYPE_SERVICE))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PROVIDER_TYPE_SERVICE_LEVEL))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PROVIDER_TO_SCENE_MINS))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PROVIDER_TO_DESTINATION_MINS))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.UNIT_NOTIFIED_BY_DISPATCH_DT))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.UNIT_ARRIVED_ON_SCENE_DT))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.UNIT_ARRIVED_TO_PATIENT_DT))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.UNIT_LEFT_SCENE_DT))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.PATIENT_ARRIVED_DESTINATION_DT))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.INJURY_FLG))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.NALOXONE_GIVEN_FLG))), ''), '|',
                        ISNULL(UPPER(LTRIM(RTRIM(b.MEDICATION_GIVEN_OTHER_FLG))), '')
                    )), 2) AS RecordHash
                FROM batch b
            )
            INSERT INTO silver.ems_clean (
                RunId, FileName, SourceRowNum,
                IncidentDttm,
                IncidentCounty,
                ChiefComplaintDispatch, ChiefComplaintAnatomicLoc,
                PrimarySymptom, ProviderImpressionPrimary,
                DispositionED, DispositionHospital, DestinationType,
                ProviderTypeStructure, ProviderTypeService, ProviderTypeServiceLevel,
                ProviderToSceneMins, ProviderToDestinationMins,
                UnitNotifiedByDispatchDttm, UnitArrivedOnSceneDttm, UnitArrivedToPatientDttm,
                UnitLeftSceneDttm, PatientArrivedDestinationDttm,
                InjuryFlg, NaloxoneGivenFlg, MedicationGivenOtherFlg,
                RecordHash
            )
            SELECT
                t.RunId, t.FileName, t.SourceRowNum,
                t.IncidentDttm,
                t.IncidentCounty,
                t.ChiefComplaintDispatch, t.ChiefComplaintAnatomicLoc,
                t.PrimarySymptom, t.ProviderImpressionPrimary,
                t.DispositionED, t.DispositionHospital, t.DestinationType,
                t.ProviderTypeStructure, t.ProviderTypeService, t.ProviderTypeServiceLevel,
                t.ProviderToSceneMins, t.ProviderToDestinationMins,
                t.UnitNotifiedByDispatchDttm, t.UnitArrivedOnSceneDttm, t.UnitArrivedToPatientDttm,
                t.UnitLeftSceneDttm, t.PatientArrivedDestinationDttm,
                t.InjuryFlg, t.NaloxoneGivenFlg, t.MedicationGivenOtherFlg,
                t.RecordHash
            FROM typed t
            LEFT JOIN rejected_keys r
              ON r.RunId = t.RunId AND r.SourceRowNum = t.SourceRowNum
            WHERE r.RunId IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM silver.ems_clean c
                  WHERE c.RecordHash = t.RecordHash
              );
            """

            # insert rejects for this batch
            cur.execute(reject_sql, batch_size, last_bronze_id)
            try:
                rows_reject_total += max(cur.rowcount or 0, 0)
            except Exception:
                pass  # some drivers return -1 for rowcount on INSERT

            # insert clean rows for this batch
            cur.execute(clean_sql, batch_size, last_bronze_id)
            try:
                rows_out_total += max(cur.rowcount or 0, 0)
            except Exception:
                pass

            # move the watermark forward to the last BronzeId in this batch
            cur.execute(
                """
                SELECT ISNULL(MAX(BronzeId), ?)
                FROM (
                    SELECT TOP (?) BronzeId
                    FROM bronze.ems_raw
                    WHERE BronzeId > ?
                    ORDER BY BronzeId
                ) x;
                """,
                last_bronze_id, batch_size, last_bronze_id
            )
            new_last = int(cur.fetchone()[0])

            if new_last == last_bronze_id:
                break  # safety check (prevents infinite loop)

            last_bronze_id = new_last
            set_last_bronze_id(conn, last_bronze_id)
            conn.commit()

        # simple "rows in" marker (max bronze seen). could be refined, but good enough for logging
        rows_in_total = max_bronze_id

        end_step(
            conn,
            step_log_id,
            "SUCCESS",
            rows_in=rows_in_total,
            rows_out=rows_out_total,
            rows_reject=rows_reject_total
        )

    except Exception as ex:
        # log failure and bubble up (so the caller can fail the run)
        try:
            end_step(
                conn,
                step_log_id,
                "FAILED",
                rows_in=rows_in_total,
                rows_out=rows_out_total,
                rows_reject=rows_reject_total,
                error_message=str(ex)
            )
        finally:
            pass
        raise
