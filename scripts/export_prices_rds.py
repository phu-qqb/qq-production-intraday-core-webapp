#!/usr/bin/env python
"""Export A-I files for a set of securities using data from SQL Server.

This is adapted from the S3-based export script but pulls intraday bars
from an MS SQL database instead of parquet files in S3.
"""
from __future__ import annotations
import argparse
import json
import pathlib
import random
import sys
from typing import List

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import sqlalchemy as sa

FMT = "%Y-%m-%d %H:%M"
OUT: dict[str, pathlib.Path]

SESSION_HOURS_UTC = {
    "US": (13, 21),  # 13:00-21:00 UTC ~ 9:00-17:00 ET
    "EU": (7, 16),   # 07:00-16:00 UTC
    "EUUS": (7, 21),
    "ALL": (0, 24),
}


def get_conn_from_secret(secret_name: str, region_name: str) -> str:
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

    user = data.get("username")
    password = data.get("password")
    host = data.get("host")
    port = data.get("port", 1433)
    db = data.get("dbname") or data.get("database") or ""
    driver = data.get("driver", "ODBC Driver 18 for SQL Server")
    return (
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{db}?driver={driver}"
    )

def parse_range(value: str) -> tuple[int, int]:
    """Return a ``(min, max)`` tuple from ``value``.

    ``value`` may be a single integer or a ``MIN-MAX``/``MIN,MAX`` range.
    """
    if "," in value:
        lo, hi = value.split(",", 1)
    elif "-" in value:
        lo, hi = value.split("-", 1)
    else:
        n = int(value)
        return n, n
    a, b = int(lo), int(hi)
    if a > b:
        a, b = b, a
    return a, b

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
) -> tuple[str, List[int]]:
    query = sa.text(
        """
        SELECT u.Name, um.SecurityId
        FROM Intraday.univ.Universe u
        JOIN Intraday.univ.UniverseMember um ON u.UniverseId = um.UniverseId
        WHERE u.Description = :desc
        """
    )
    df = pd.read_sql(query, engine, params={"desc": description})
    if df.empty:
        return description, []
    name = df["Name"].iloc[0]
    return name, df["SecurityId"].tolist()

def read_price_bars(
    engine: sa.engine.Engine,
    security_id: int,
    start: str | None,
    end: str | None,
    session: str,
    timeframe: int = 30,
) -> pd.DataFrame:
    params = {"sid": security_id, "tf": timeframe}
    sql = (
        "SELECT BarTimeUtc AS timestamp, Close AS close "
        "FROM Intraday.mkt.PriceBar "
        "WHERE SecurityId = :sid AND TimeframeMinute = :tf"
    )
    if start:
        sql += " AND BarTimeUtc >= :start"
        params["start"] = start
    if end:
        sql += " AND BarTimeUtc <= :end"
        params["end"] = end
    sql += " ORDER BY BarTimeUtc"
    df = pd.read_sql(sa.text(sql), engine, params=params)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if session != "ALL":
        lo, hi = SESSION_HOURS_UTC[session]
        mask = df["timestamp"].dt.hour.between(lo, hi - 1)
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
cli.add_argument("--offset", type=int, default=0)
cli.add_argument("--limit", type=int, default=50)
cli.add_argument("--symbols-file")
cli.add_argument("--start")
cli.add_argument("--end")
cli.add_argument("--seed", type=int, default=42)
cli.add_argument("--subN", type=int, default=150)
cli.add_argument(
    "--count",
    type=str,
    help=(
        "Securities per subscriber in F.txt. Provide a single integer for a "
        "fixed size or MIN-MAX to randomise each sub-universe. If omitted, a "
        "random size between min(10, n-2) and n - min(10, n-2) is used "
        "where n is the universe size."
    ),
)
args = cli.parse_args()

conn_str = args.conn or get_conn_from_secret(args.secret_name, args.region)
engine = sa.create_engine(conn_str)

universe_name, universe_ids = get_universe_info(engine, args.universe)
output_dir = pathlib.Path("src/TradingDaemon/Data/Universes") / universe_name
output_dir.mkdir(parents=True, exist_ok=True)
OUT = {k: output_dir / f"{k}.txt" for k in "ABCDEFGHI"}
for path in OUT.values():
    if path.exists():
        path.unlink()

def load_security_ids(universe_ids: List[int]) -> List[int]:
    if args.symbols_file:
        with open(args.symbols_file) as fh:
            wanted = {int(ln.strip()) for ln in fh if ln.strip()}
        subset = [sid for sid in universe_ids if sid in wanted]
        print(f"Loaded {len(subset)} securities from {args.symbols_file}")
    else:
        subset = universe_ids[args.offset : args.offset + args.limit]
    return subset

subset = load_security_ids(universe_ids)
if not subset:
    sys.exit("No securities selected")

min_sub_size = max(1, min(10, len(subset) - 2))
count_range = None
if args.count is not None:
    count_range = parse_range(args.count)
    if count_range[0] < min_sub_size:
        cli.error(f"--count must be >= {min_sub_size}")

rng = random.Random(args.seed)
sec_ids: List[int] = []
all_ts: set[pd.Timestamp] = set()
first_G = True
sid_next = 100000

for real_sid in subset:
    sid = sid_next
    sid_next += 1
    sec_ids.append(sid)
    print("→", real_sid)

    df_raw = read_price_bars(engine, real_sid, args.start, args.end, args.session)
    check_long_gaps(df_raw["timestamp"], 5)
    if df_raw.empty:
        continue

    raw = df_raw.set_index("timestamp")["close"]
    flat = raw.resample("30T").ffill()
    all_ts.update(raw.index)

    frame(sid, flat).to_csv(OUT["A"], mode="a", header=False, index=False)

    fraw = frame(sid, raw)
    fraw.to_csv(OUT["H"], mode="a", header=False, index=False)
    fraw.to_csv(OUT["I"], mode="a", header=False, index=False)

    if first_G:
        frame(sid, raw).to_csv(OUT["G"], header=False, index=False)
        first_G = False

# Auxiliary B C D E F
pd.Series(sec_ids).to_csv(OUT["B"], header=False, index=False)

ts_sorted = sorted(all_ts)
pd.Series(pd.to_datetime(ts_sorted).strftime(FMT)).to_csv(OUT["D"], header=False, index=False)
with OUT["C"].open("w") as fhc:
    for t in pd.to_datetime(ts_sorted).strftime(FMT):
        for sid in sec_ids:
            fhc.write(f"{sid},{t}\n")

subs = list(range(21000, 21000 + args.subN))
pd.Series(subs).to_csv(OUT["E"], header=False, index=False)
with OUT["F"].open("w") as fhf:
    for su in subs:
        if count_range is not None:
            lo, hi = count_range
            hi = min(hi, len(sec_ids))
            lo = min(lo, hi)
            k = rng.randint(lo, hi)
            chosen = sec_ids if k >= len(sec_ids) else rng.sample(sec_ids, k)
        else:
            k = rng.randint(
                min_sub_size, max(min_sub_size, len(sec_ids) - min_sub_size)
            )
            chosen = rng.sample(sec_ids, k)
        for sid in chosen:
            fhf.write(f"{su},{sid}\n")

print("✅  A–I.txt générés")
