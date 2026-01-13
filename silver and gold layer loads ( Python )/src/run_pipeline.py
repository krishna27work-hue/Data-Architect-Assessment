import argparse
import json
import os
import sys

from .db import connect
from .silver import run_silver
from .gold import run_gold


def parse_args():
    # CLI args so SSIS (or cmd) can trigger the same pipeline without code changes
    p = argparse.ArgumentParser()
    p.add_argument("--conn", required=True, help="ODBC connection string for SQL Server")
    p.add_argument("--run-id", required=True, help="RunId (GUID string) used for step logging")
    p.add_argument("--full-refresh", action="store_true",
                   help="Rebuild silver & gold from scratch and reset watermark")
    p.add_argument("--silver-only", action="store_true", help="Run only the silver step")
    p.add_argument("--gold-only", action="store_true", help="Run only the gold step")
    p.add_argument("--batch-size", type=int, default=50000,
                   help="Bronze batch size per loop (default 50000)")
    return p.parse_args()


def main():
    args = parse_args()

    # connect once and reuse for both steps
    conn = connect(args.conn)

    # avoid conflicting switches
    if args.gold_only and args.silver_only:
        raise SystemExit("Choose at most one of --silver-only or --gold-only")

    # run modes:
    # - gold-only: just publish DW tables from existing silver
    # - silver-only: just build silver tables from bronze
    # - default: run silver then gold
    if args.gold_only:
        run_gold(conn, args.run_id, full_refresh=args.full_refresh)
    elif args.silver_only:
        run_silver(conn, args.run_id, batch_size=args.batch_size, full_refresh=args.full_refresh)
    else:
        run_silver(conn, args.run_id, batch_size=args.batch_size, full_refresh=args.full_refresh)
        run_gold(conn, args.run_id, full_refresh=args.full_refresh)

    # keep output simple for SSIS Execute Process Task
    print("OK")


if __name__ == "__main__":
    main()
