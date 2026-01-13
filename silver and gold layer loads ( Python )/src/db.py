import pyodbc

def connect(conn_str: str) -> pyodbc.Connection:
    """Connect using a full ODBC connection string."""
    conn = pyodbc.connect(conn_str, autocommit=False)
    return conn
