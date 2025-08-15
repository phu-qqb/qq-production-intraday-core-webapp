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
) -> tuple[int, str, pd.DataFrame]:
    query = sa.text(
        """
        SELECT u.UniverseId, u.Name, um.SecurityId, um.EffectiveFromUtc, um.EffectiveToUtc
        FROM Intraday.univ.Universe u
        JOIN Intraday.univ.UniverseMember um ON u.UniverseId = um.UniverseId
        WHERE u.Name = :desc
        """
    )
    df = pd.read_sql(query, engine, params={"desc": description})
    if df.empty:
        return 0, description, df
    uid = int(df["UniverseId"].iloc[0])
    name = df["Name"].iloc[0]
    return uid, name, df[["SecurityId", "EffectiveFromUtc", "EffectiveToUtc"]]


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
    df = df[mask].copy()
    df["timestamp"] = ts[mask]
    return df


def read_flat_bars(
    engine: sa.engine.Engine,
    security_id: int,
    start: str | None,
    session: str,
    timeframe: int = 60,
) -> pd.DataFrame:
    params = {"sid": security_id, "tf": timeframe}
    sql = (
        "SELECT BarTimeUtc AS timestamp, [Close] AS [close] "
        "FROM Intraday.mkt.FlatBar "
        "WHERE SecurityId = :sid AND TimeframeMinute = :tf"
    )
    if start:
        sql += " AND BarTimeUtc >= :start"
        params["start"] = start
    sql += " ORDER BY BarTimeUtc"
    df = pd.read_sql(sa.text(sql), engine, params=params)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    start_t, end_t = SESSION_HOURS_NY[session]
    ts = df["timestamp"].dt.tz_convert("America/New_York")
    minutes = ts.dt.hour * 60 + ts.dt.minute
    lo = start_t.hour * 60 + start_t.minute
    hi = end_t.hour * 60 + end_t.minute
    mask = minutes.between(lo, hi)
    df = df[mask].copy()
    df["timestamp"] = ts[mask]
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

universe_id, universe_name, members_df = get_universe_info(engine, args.universe)
universe_ids = members_df["SecurityId"].unique().tolist()
membership_by_real_sid: dict[int, List[tuple[pd.Timestamp, pd.Timestamp]]] = {}
for row in members_df.itertuples(index=False):
    start = pd.to_datetime(row.EffectiveFromUtc, utc=True)
    end = pd.to_datetime(row.EffectiveToUtc, utc=True, errors="coerce")
    if pd.isna(end):
        end = pd.Timestamp.max.tz_localize("UTC")
    membership_by_real_sid.setdefault(row.SecurityId, []).append((start, end))
# Save exported price files to a fixed Windows directory for downstream processes
# that expect universes to reside under ``C:\IntradayFX``.
output_dir = pathlib.Path(r"C:\IntradayFX") / universe_name
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

all_ts: set[pd.Timestamp] = set()
first_G = True

for real_sid in universe_ids:
    sid = real_sid
    print("→", real_sid)

    df_raw = read_price_bars(
        engine, real_sid, args.start, args.session, args.timeframe
    )
    check_long_gaps(df_raw["timestamp"], 5)
    if df_raw.empty:
        print(f"Skipping {real_sid}: no raw bars")
        continue

    df_flat = read_flat_bars(
        engine, real_sid, args.start, args.session, args.timeframe
    )
    if df_flat.empty:
        print(f"Skipping {real_sid}: no flat bars")
        continue

    raw = df_raw.set_index("timestamp")["close"]
    flat = df_flat.set_index("timestamp")["close"]
    all_ts.update(raw.index)

    flat_frame = frame(sid, flat)
    print(f"Writing {len(flat_frame)} rows to {OUT['A']}")
    flat_frame.to_csv(OUT["A"], mode="a", header=False, index=False)

    fraw = frame(sid, raw)
    print(f"Writing {len(fraw)} rows to {OUT['H']} and {OUT['I']}")
    fraw.to_csv(OUT["H"], mode="a", header=False, index=False)
    fraw.to_csv(OUT["I"], mode="a", header=False, index=False)

    if first_G:
        fraw.to_csv(OUT["G"], header=False, index=False)
        first_G = False

# Auxiliary B C D
pd.Series(universe_ids).to_csv(OUT["B"], header=False, index=False)

ts_sorted = sorted(all_ts)
ts_fmt = pd.to_datetime(ts_sorted).strftime(FMT)
pd.Series(ts_fmt).to_csv(OUT["D"], header=False, index=False)
with OUT["C"].open("w") as fhc:
    for t, t_str in zip(ts_sorted, ts_fmt):
        for real_sid, intervals in membership_by_real_sid.items():
            for start, end in intervals:
                if start <= t <= end:
                    fhc.write(f"{real_sid},{t_str}\n")
                    break

for key in ["A", "H", "I"]:
    path = OUT[key]
    if path.exists():
        print(f"Created {path} ({path.stat().st_size} bytes)")
    else:
        print(f"Warning: expected {path} was not created")

print("✅  Export complete")
