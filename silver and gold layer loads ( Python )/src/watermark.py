import pyodbc

# one watermark per pipeline so we can do safe incremental runs
PIPELINE_NAME = "ems_silver_gold"


def ensure_watermark_table(conn: pyodbc.Connection) -> None:
    # creates etl schema + watermark table if it doesn't exist (dev-friendly)
    cur = conn.cursor()
    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.schemas s
            WHERE s.name = 'etl'
        )
        BEGIN
            EXEC('CREATE SCHEMA etl');
        END

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = 'etl' AND t.name = 'watermark'
        )
        BEGIN
            CREATE TABLE etl.watermark (
                PipelineName NVARCHAR(100) NOT NULL PRIMARY KEY,
                LastBronzeId BIGINT NOT NULL,
                UpdatedUtc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
            );
        END
        """
    )
    conn.commit()


def get_last_bronze_id(conn: pyodbc.Connection, pipeline_name: str = PIPELINE_NAME) -> int:
    # reads last processed BronzeId so silver only picks up new rows
    ensure_watermark_table(conn)

    cur = conn.cursor()
    cur.execute("SELECT LastBronzeId FROM etl.watermark WHERE PipelineName = ?;", pipeline_name)
    row = cur.fetchone()

    if not row:
        # first run for this pipeline -> start from 0
        cur.execute(
            "INSERT INTO etl.watermark (PipelineName, LastBronzeId) VALUES (?, 0);",
            pipeline_name
        )
        conn.commit()
        return 0

    return int(row[0])


def set_last_bronze_id(conn: pyodbc.Connection, last_bronze_id: int, pipeline_name: str = PIPELINE_NAME) -> None:
    # updates the watermark after a successful run (MERGE keeps it idempotent)
    ensure_watermark_table(conn)

    cur = conn.cursor()
    cur.execute(
        """
        MERGE etl.watermark AS tgt
        USING (SELECT ? AS PipelineName, ? AS LastBronzeId) AS src
          ON tgt.PipelineName = src.PipelineName
        WHEN MATCHED THEN
          UPDATE SET LastBronzeId = src.LastBronzeId, UpdatedUtc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
          INSERT (PipelineName, LastBronzeId) VALUES (src.PipelineName, src.LastBronzeId);
        """,
        pipeline_name, last_bronze_id
    )
    conn.commit()
