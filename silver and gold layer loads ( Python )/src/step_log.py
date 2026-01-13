import pyodbc


def start_step(conn: pyodbc.Connection, run_id: str, step_name: str) -> int:
    # insert a STARTED record for this step and return the StepLogId
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO etl.run_step_log (RunId, StepName, Status)
        OUTPUT INSERTED.StepLogId
        VALUES (?, ?, 'STARTED');
        """,
        run_id, step_name
    )
    step_log_id = int(cur.fetchone()[0])
    conn.commit()
    return step_log_id


def end_step(
    conn: pyodbc.Connection,
    step_log_id: int,
    status: str,
    rows_in: int | None = None,
    rows_out: int | None = None,
    rows_reject: int | None = None,
    error_message: str | None = None
) -> None:
    # update the same step row when the step finishes (SUCCESS/FAILED + counts)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE etl.run_step_log
        SET EndedUtc = SYSUTCDATETIME(),
            Status = ?,
            RowsIn = ?,
            RowsOut = ?,
            RowsReject = ?,
            ErrorMessage = ?
        WHERE StepLogId = ?;
        """,
        status, rows_in, rows_out, rows_reject, error_message, step_log_id
    )
    conn.commit()
