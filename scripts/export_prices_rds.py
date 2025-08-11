#!/usr/bin/env python
"""Export pricing files for a set of securities using data from SQL Server.

This is adapted from the S3-based export script but pulls intraday bars
from an MS SQL database instead of parquet files in S3.
"""
from __future__ import annotations
import argparse
import json
import pathlib
import sys
from datetime import time
from typing import List

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote_plus

FMT = "%Y-%m-%d %H:%M"
OUT: dict[str, pathlib.Path]

# Session boundaries defined in New York time (handles daylight saving)
SESSION_HOURS_NY = {
    "US": (time(9, 30), time(15, 59)),
    "EU": (time(2, 0), time(8, 59)),
    "EUUS": (time(2, 0), time(11, 59)),
    "ALL": (time(2, 0), time(15, 59)),
}


def get_conn_from_secret(
    secret_name: str, region_name: str, default_driver: str
) -> str:
    """Return an SQLAlchemy connection string from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        raise RuntimeError(f"Failed to retrieve secret {secret_name}") from exc

    secret_str = resp.get("SecretString", "")
    data = json.loads(secret_str)

    # Allow the secret to contain a full connection string or components.
    if "conn" in data:
        return data["conn"]

    user = quote_plus(data.get("username", ""))
    password = quote_plus(data.get("password", ""))
    host = data.get("host")
    port = data.get("port", 1433)
    db = quote_plus(data.get("dbname") or data.get("database") or "")

    driver = quote_plus(data.get("driver", default_driver))
    return (
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{db}?driver={driver}&Encrypt=no"
    )


def check_long_gaps(ts: pd.Series, limit_days: int = 5) -> None:
    days = (
        pd.to_datetime(ts)
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    if days.empty:
        return
    diffs = days.diff().dt.days
    gaps = diffs[diffs > limit_days]
    for idx in gaps.index:
        start = days.iloc[idx - 1].date()
        end = days.iloc[idx].date()
        print(f"Warning: gap {start} → {end} ({int(gaps.loc[idx])} days)")

def frame(sec_id: int, ser: pd.Series) -> pd.DataFrame:
    df = ser.rename("price").reset_index().rename(columns={"index": "timestamp"})
    df.insert(0, "securityId", sec_id)
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime(FMT)
    return df

def get_universe_info(
    engine: sa.engine.Engine, description: str
) -> tuple[int, str, List[int]]:
    query = sa.text(
        """
        SELECT u.UniverseId, u.Name, um.SecurityId
        FROM Intraday.univ.Universe u
        JOIN Intraday.univ.UniverseMember um ON u.UniverseId = um.UniverseId
        WHERE u.Name = :desc
        """
    )
    df = pd.read_sql(query, engine, params={"desc": description})
    if df.empty:
        return 0, description, []
    uid = int(df["UniverseId"].iloc[0])
    name = df["Name"].iloc[0]
    return uid, name, df["SecurityId"].tolist()


def get_subuniverse_data(
    engine: sa.engine.Engine, universe_id: int
) -> tuple[List[int], pd.DataFrame]:
    sub_df = pd.read_sql(
        sa.text(
            "SELECT SubUniverseId FROM Intraday.univ.SubUniverse WHERE UniverseId = :uid"
        ),
        engine,
        params={"uid": universe_id},
    )
    sub_ids = sub_df["SubUniverseId"].tolist()
    if not sub_ids:
        return [], pd.DataFrame(columns=["SubUniverseId", "SecurityId"])
    ids_str = ",".join(str(i) for i in sub_ids)
    members_df = pd.read_sql(
        sa.text(
            "SELECT SubUniverseId, SecurityId FROM Intraday.univ.SubUniverseMember "
            f"WHERE SubUniverseId IN ({ids_str})"
        ),
        engine,
    )
    return sub_ids, members_df

def read_price_bars(
    engine: sa.engine.Engine,
    security_id: int,
    start: str | None,
    session: str,
    timeframe: int = 60,
) -> pd.DataFrame:
    params = {"sid": security_id, "tf": timeframe}
    sql = (
        "SELECT BarTimeUtc AS timestamp, [Close] AS [close] "
        "FROM Intraday.mkt.PriceBar "
        "WHERE SecurityId = :sid AND TimeframeMinute = :tf"
    )
    if start:
        sql += " AND BarTimeUtc >= :start"
        params["start"] = start
    sql += " ORDER BY BarTimeUtc"
    df = pd.read_sql(sa.text(sql), engine, params=params)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    start, end = SESSION_HOURS_NY[session]
    ts = df["timestamp"].dt.tz_convert("America/New_York")
    minutes = ts.dt.hour * 60 + ts.dt.minute
    lo = start.hour * 60 + start.minute
    hi = end.hour * 60 + end.minute
    mask = minutes.between(lo, hi)
    df = df[mask]
    return df

# ---------- CLI ----------
cli = argparse.ArgumentParser()
cli.add_argument("--session", choices=["US", "EU", "EUUS", "ALL"], default="EUUS")
cli.add_argument("--universe", required=True, help="Universe description")
cli.add_argument(
    "--conn",
    help="SQLAlchemy connection string (overrides AWS secret if provided)",
)
cli.add_argument(
    "--secret-name",
    default="qq-intraday-credentials",
    help="AWS Secrets Manager name containing DB credentials",
)
cli.add_argument(
    "--region",
    default="eu-west-2",
    help="AWS region where the secret is stored",
)
cli.add_argument(
    "--driver",
    default="ODBC Driver 17 for SQL Server",
    help="ODBC driver name to use when connecting via pyodbc",
)
cli.add_argument("--start")
cli.add_argument(
    "--timeframe",
    type=int,
    default=60,
    help="Bar timeframe in minutes (TimeframeMinute)",
)
args = cli.parse_args()

conn_str = args.conn or get_conn_from_secret(args.secret_name, args.region, args.driver)
engine = sa.create_engine(conn_str)

universe_id, universe_name, universe_ids = get_universe_info(engine, args.universe)
output_dir = pathlib.Path("src/TradingDaemon/Data/Universes") / universe_name
output_dir.mkdir(parents=True, exist_ok=True)
OUT = {k: output_dir / f"{k}.txt" for k in "ABCDEFGHI"}
for path in OUT.values():
    if path.exists():
        path.unlink()

if not universe_ids:
    sys.exit("No securities selected")

sub_ids, sub_members = get_subuniverse_data(engine, universe_id)
pd.Series(sub_ids).to_csv(OUT["E"], header=False, index=False)
sub_members.to_csv(OUT["F"], header=False, index=False)

sec_ids: List[int] = []
all_ts: set[pd.Timestamp] = set()
first_G = True
sid_next = 100000

for real_sid in universe_ids:
    sid = sid_next
    sid_next += 1
    sec_ids.append(sid)
    print("→", real_sid)

    df_raw = read_price_bars(engine, real_sid, args.start, args.session, args.timeframe)
    check_long_gaps(df_raw["timestamp"], 5)
    if df_raw.empty:
        continue

    raw = df_raw.set_index("timestamp")["close"]
    flat = raw.resample(f"{args.timeframe}T").ffill()
    all_ts.update(raw.index)

    frame(sid, flat).to_csv(OUT["A"], mode="a", header=False, index=False)

    fraw = frame(sid, raw)
    fraw.to_csv(OUT["H"], mode="a", header=False, index=False)
    fraw.to_csv(OUT["I"], mode="a", header=False, index=False)

    if first_G:
        frame(sid, raw).to_csv(OUT["G"], header=False, index=False)
        first_G = False

# Auxiliary B C D
pd.Series(sec_ids).to_csv(OUT["B"], header=False, index=False)

ts_sorted = sorted(all_ts)
pd.Series(pd.to_datetime(ts_sorted).strftime(FMT)).to_csv(OUT["D"], header=False, index=False)
with OUT["C"].open("w") as fhc:
    for t in pd.to_datetime(ts_sorted).strftime(FMT):
        for sid in sec_ids:
            fhc.write(f"{sid},{t}\n")

print("✅  Export complete")
