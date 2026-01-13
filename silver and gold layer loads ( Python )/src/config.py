import json
from dataclasses import dataclass
from .db import SqlServerConfig


@dataclass
class AppConfig:
    # Central app config object used across the pipeline
    sql_server: SqlServerConfig
    run_id: str
    batch_size: int = 50000  # used for chunking/bulk patterns on large files
    dry_run: bool = False    # when true, run logic without writing to DB


def load_config(path: str) -> AppConfig:
    """Load pipeline config from a JSON file and return a typed AppConfig."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Read SQL Server settings (supports defaults where reasonable)
    ss = raw["sql_server"]
    sql_cfg = SqlServerConfig(
        driver=ss.get("driver", "{ODBC Driver 18 for SQL Server}"),
        server=ss["server"],                         # required
        database=ss.get("database", "ems"),
        trusted_connection=bool(ss.get("trusted_connection", True)),
        username=ss.get("username", ""),
        password=ss.get("password", ""),
        # Keep these explicit because local/dev SQL Server setups vary
        encrypt=ss.get("encrypt", "no"),
        trust_server_certificate=ss.get("trust_server_certificate", "yes"),
    )

    # run_id is required so Silver/Gold can tie back to the same SSIS/bronze run
    return AppConfig(
        sql_server=sql_cfg,
        run_id=str(raw["run_id"]),
        batch_size=int(raw.get("batch_size", 50000)),
        dry_run=bool(raw.get("dry_run", False)),
    )


