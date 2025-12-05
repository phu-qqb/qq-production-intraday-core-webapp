#!/usr/bin/env python
"""Export pricing files for a set of securities using data from SQL Server.

This is adapted from the S3-based export script but pulls intraday bars
from an MS SQL database instead of parquet files in S3.
"""
from __future__ import annotations
import argparse
import json
import numpy as np
import os
import pathlib
import sys
import time as perf_time
from datetime import time
from typing import List
import re

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote_plus

FMT = "%Y-%m-%d %H:%M"
OUT: dict[str, pathlib.Path]


def log_timer(label: str, start: float) -> float:
    now = perf_time.perf_counter()
    print(f"{label} took {now - start:.2f}s")
    return now

# Session boundaries defined in New York time (handles daylight saving).
# Keep these aligned with the TradingDaemon session configuration used by the
# webapp (see PriceFetcher/WakettPriceFetcher/WeightCalculator). Those
# components treat sessions as inclusive of the start minute and exclusive of
# the minute after ``End``; the same convention is used here when filtering
# bar timestamps.
SESSION_HOURS_NY = {
    "US": (time(9, 30), time(16, 14)),
    "EU": (time(2, 0), time(8, 59)),
    "EUUS": (time(2, 0), time(11, 59)),
    # "ALL" is not used by the webapp but is kept here for completeness to
    # cover the entire trading day handled by the other sessions.
    "ALL": (time(2, 0), time(15, 59)),
}


def resolve_home_root() -> pathlib.Path:
    env_root = os.environ.get("HOME_ROOT")
    if env_root:
        return pathlib.Path(env_root)

    environment = (
        os.environ.get("ASPNETCORE_ENVIRONMENT")
        or os.environ.get("DOTNET_ENVIRONMENT")
        or ""
    ).lower()

    if environment == "test":
        return pathlib.Path("/home_test")

    return pathlib.Path("/home")


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
        print(f"Warning: gap {start} to {end} ({int(gaps.loc[idx])} days)")

def frame(sec_id: int, ser: pd.Series) -> pd.DataFrame:
    df = ser.rename("price").reset_index().rename(columns={"index": "timestamp"})
    df.insert(0, "securityId", sec_id)
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("UTC").dt.strftime(FMT)
    )
    return df


def normalize_symbol(symbol: str) -> str:
    return symbol.replace("/", "").strip().upper()


def parse_pair(symbol: str) -> tuple[str, str] | None:
    normalized = normalize_symbol(symbol)
    if len(normalized) != 6:
        return None
    return normalized[:3], normalized[3:]


def _strip_json_comments(raw: str) -> str:
    """Remove ``//`` and ``/* */`` comments from JSON text safely."""

    out = []
    in_string = False
    escape = False
    i = 0
    length = len(raw)

    while i < length:
        ch = raw[i]

        if escape:
            out.append(ch)
            escape = False
            i += 1
            continue

        if ch == "\\" and in_string:
            out.append(ch)
            escape = True
            i += 1
            continue

        if ch == '"':
            in_string = not in_string
            out.append(ch)
            i += 1
            continue

        if not in_string:
            if raw.startswith("//", i):
                newline = raw.find("\n", i)
                if newline == -1:
                    break
                i = newline + 1
                continue

            if raw.startswith("/*", i):
                end = raw.find("*/", i + 2)
                if end == -1:
                    break
                i = end + 2
                continue

        out.append(ch)
        i += 1

    return "".join(out)


def _load_json_allowing_comments(path: pathlib.Path) -> dict:
    """Load JSON that may contain ``//`` or ``/* */`` comments."""

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    cleaned = _strip_json_comments(path.read_text())
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse JSON from {path}: {exc}") from exc


def load_configured_base_pairs(path: pathlib.Path) -> list[str]:
    data = _load_json_allowing_comments(path)

    base_pairs = (
        data.get("ExternalApis", {})
        .get("WakettApi", {})
        .get("BasePairs", [])
    )
    return [normalize_symbol(p) for p in base_pairs if isinstance(p, str)]


def ensure_usd_quote(pair: str) -> tuple[str | None, bool]:
    parsed = parse_pair(pair)
    if parsed is None:
        return None, False

    base, quote = parsed
    if quote == "USD":
        return pair, False
    if base == "USD":
        return f"{quote}USD", True
    return None, False


def load_security_definitions(engine: sa.engine.Engine) -> pd.DataFrame:
    sql = sa.text(
        """
        SELECT SecurityId, Symbol
        FROM core.Security
        WHERE IsActive = 1 AND Symbol IS NOT NULL AND LTRIM(RTRIM(Symbol)) <> ''
        """
    )
    df = pd.read_sql(sql, engine)
    df["NormalizedSymbol"] = df["Symbol"].apply(normalize_symbol)
    return df


def build_base_currency_series(
    engine: sa.engine.Engine,
    base_pairs: list[str],
    security_defs: pd.DataFrame,
    start: str | None,
    session: str,
    timeframe: int,
) -> dict[str, pd.Series]:
    series_map: dict[str, pd.Series] = {}
    lookup = {
        row["NormalizedSymbol"]: int(row["SecurityId"])
        for _, row in security_defs.iterrows()
    }

    for configured in base_pairs:
        target_symbol, invert = ensure_usd_quote(configured)
        if target_symbol is None:
            print(f"Skipping base pair without USD: {configured}")
            continue

        candidates = [target_symbol]
        if configured not in candidates:
            candidates.append(configured)

        security_id = None
        used_symbol = None
        used_inversion = invert

        for candidate in candidates:
            sid = lookup.get(candidate)
            if sid is not None:
                security_id = sid
                used_symbol = candidate
                used_inversion = candidate != target_symbol
                break

        if security_id is None or used_symbol is None:
            print(f"No security found for base pair {configured}")
            continue

        raw_df = read_price_bars(engine, security_id, start, session, timeframe)
        raw_series = raw_df.set_index("timestamp")["close"]
        if used_inversion:
            raw_series = 1 / raw_series

        currency = target_symbol[:3]
        series_map[currency] = raw_series
        check_long_gaps(raw_series.index.to_series(), 5)
        print(
            f"Loaded base {currency}USD from security {security_id}"
            f" ({'inverted' if used_inversion else 'direct'} {used_symbol})"
        )

    return series_map


def build_security_symbol_map(
    security_defs: pd.DataFrame, universe_ids: list[int]
) -> dict[int, str]:
    subset = security_defs[security_defs["SecurityId"].isin(universe_ids)]
    return {
        int(row["SecurityId"]): row["NormalizedSymbol"] for _, row in subset.iterrows()
    }


def build_currency_frame(currency_usd: dict[str, pd.Series]) -> pd.DataFrame:
    if not currency_usd:
        return pd.DataFrame()

    return pd.concat(currency_usd, axis=1, join="outer").sort_index()


def compute_series_for_symbol(
    pair: tuple[str, str], currency_frame: pd.DataFrame
) -> pd.Series | None:
    base, quote = pair
    if quote == "USD":
        series = currency_frame.get(base)
        return None if series is None else series.dropna()
    if base == "USD":
        series = currency_frame.get(quote)
        return None if series is None else (1 / series).dropna()

    base_series = currency_frame.get(base)
    quote_series = currency_frame.get(quote)
    if base_series is None or quote_series is None:
        return None

    ratio = base_series / quote_series
    return ratio.dropna()


def flatten_series(raw: pd.Series, tz: str = "America/New_York") -> pd.Series:
    if raw.empty:
        return raw

    ordered = raw.sort_index()
    local_dates = ordered.index.tz_convert(tz).date
    returns = ordered.pct_change().fillna(0)
    day_change = pd.Series(local_dates).ne(pd.Series(local_dates).shift())
    returns.loc[day_change.values] = 0

    forward_factors = (1 + returns.iloc[1:]).to_numpy()
    if forward_factors.size:
        forward_products = np.cumprod(forward_factors[::-1])[::-1]
        factors = np.concatenate([forward_products, np.array([1.0])])
    else:
        factors = np.array([1.0])

    flattened_values = ordered.iloc[-1] / factors
    return pd.Series(flattened_values, index=ordered.index)

def get_universe_info(
    engine: sa.engine.Engine, description: str
) -> tuple[int, str, pd.DataFrame]:
    query = sa.text(
        """
        SELECT u.UniverseId, u.Name, um.SecurityId, um.EffectiveFromUtc, um.EffectiveToUtc
        FROM univ.Universe u
        JOIN univ.UniverseMember um ON u.UniverseId = um.UniverseId
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
            "SELECT SubUniverseId FROM univ.SubUniverse WHERE UniverseId = :uid"
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
            "SELECT SubUniverseId, SecurityId FROM univ.SubUniverseMember "
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
        "FROM mkt.PriceBar "
        "WHERE SecurityId = :sid AND TimeframeMinute = :tf "
        "AND DATEPART(MINUTE, BarTimeUtc) % :tf = 6"
    )
    if start:
        sql += " AND BarTimeUtc >= :start"
        params["start"] = start
    sql += " ORDER BY BarTimeUtc"
    df = pd.read_sql(sa.text(sql), engine, params=params)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    end_times = df["timestamp"] + pd.Timedelta(minutes=timeframe)
    start, end = SESSION_HOURS_NY[session]
    local = end_times.dt.tz_convert("America/New_York")
    minutes = local.dt.hour * 60 + local.dt.minute
    lo = start.hour * 60 + start.minute
    hi = end.hour * 60 + end.minute + 1
    mask = minutes.between(lo, hi)
    return df[mask].copy()


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
cli.add_argument(
    "--start",
    help="Start date in America/New_York timezone (bars from this date onward)",
)
cli.add_argument(
    "--timeframe",
    type=int,
    default=60,
    help="Bar timeframe in minutes (TimeframeMinute)",
)

_DEFAULT_CONFIG_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "src"
    / "TradingDaemon"
    / "appsettings.json"
)

cli.add_argument(
    "--config",
    type=pathlib.Path,
    default=_DEFAULT_CONFIG_PATH,
    help=(
        "Path to appsettings.json containing ExternalApis:WakettApi:BasePairs. "
        "Defaults to the repository copy if available."
    ),
)
args = cli.parse_args()
print("timeframe:", args.timeframe)
print("session:", args.session)
print("universe:", args.universe)
print("secrets:", args.secret_name)

t_total_start = perf_time.perf_counter()
conn_str = args.conn or get_conn_from_secret(args.secret_name, args.region, args.driver)
engine = sa.create_engine(conn_str)
t_mark = log_timer("Create engine", t_total_start)

start_filter = None
if args.start:
    start_dt = pd.Timestamp(args.start)
    if start_dt.tzinfo is None:
        start_dt = start_dt.tz_localize("America/New_York")
    else:
        start_dt = start_dt.tz_convert("America/New_York")
    start_dt = start_dt.normalize()
    start_filter = start_dt.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")

universe_id, universe_name, members_df = get_universe_info(engine, args.universe)
t_mark = log_timer("Fetch universe info", t_mark)
universe_ids = members_df["SecurityId"].unique().tolist()
print("Universe ID:", universe_id)
# Save exported price files to a fixed directory for downstream processes
# that expect universes to reside under the ``HOME_ROOT`` directory.
home_root = resolve_home_root()
output_dir = home_root / "data" / "historical_data" / f"Univ{universe_id}"
output_dir.mkdir(parents=True, exist_ok=True)
OUT = {k: output_dir / f"{k}.txt" for k in "ABCDEFGHI"}
for path in OUT.values():
    if path.exists():
        path.unlink()

if not universe_ids:
    sys.exit("No securities selected")

sub_ids, sub_members = get_subuniverse_data(engine, universe_id)
t_mark = log_timer("Fetch subuniverse data", t_mark)
pd.Series(sub_ids).to_csv(OUT["E"], header=False, index=False)
sub_members.to_csv(OUT["F"], header=False, index=False)

all_ts: set[pd.Timestamp] = set()
first_G = True
flat_frames: list[pd.DataFrame] = []
raw_frames: list[pd.DataFrame] = []
g_frame: pd.DataFrame | None = None

base_pairs = load_configured_base_pairs(args.config)
t_mark = log_timer("Load base pairs from config", t_mark)
if not base_pairs:
    sys.exit("No base pairs configured in appsettings.json")

security_defs = load_security_definitions(engine)
t_mark = log_timer("Load security definitions", t_mark)
currency_usd = build_base_currency_series(
    engine, base_pairs, security_defs, start_filter, args.session, args.timeframe
)
t_mark = log_timer("Build base USD currency series", t_mark)
if not currency_usd:
    sys.exit("No base USD pairs available to build prices")

symbol_map = build_security_symbol_map(security_defs, universe_ids)
t_mark = log_timer("Build security symbol map", t_mark)
currency_frame = build_currency_frame(currency_usd)
t_mark = log_timer("Build currency frame", t_mark)
if currency_frame.empty:
    sys.exit("No currency data available to build prices")

for real_sid in universe_ids:
    sym_start = perf_time.perf_counter()
    sid = real_sid
    symbol = symbol_map.get(sid)
    if not symbol:
        print(f"Skipping {sid}: no symbol found")
        continue

    parsed = parse_pair(symbol)
    if parsed is None:
        print(f"Skipping {sid}: invalid symbol {symbol}")
        continue

    print("Processing", sid, symbol)
    raw = compute_series_for_symbol(parsed, currency_frame)
    if raw is None or raw.empty:
        print(f"Skipping {sid}: unable to build raw series")
        continue

    check_long_gaps(raw.index.to_series(), 5)
    flat = flatten_series(raw)
    all_ts.update(flat.index)

    flat_frame = frame(sid, flat)
    flat_frames.append(flat_frame)

    fraw = frame(sid, raw)
    raw_frames.append(fraw)

    if first_G:
        g_frame = fraw
        first_G = False

    log_timer(f"Processed {sid} {symbol}", sym_start)

if flat_frames:
    t_write = perf_time.perf_counter()
    flat_all = pd.concat(flat_frames, ignore_index=True)
    print(f"Writing {len(flat_all)} flattened rows to {OUT['A']}")
    flat_all.to_csv(OUT["A"], header=False, index=False)
    t_write = log_timer("Write flattened prices", t_write)

if raw_frames:
    t_write_raw = perf_time.perf_counter()
    raw_all = pd.concat(raw_frames, ignore_index=True)
    print(f"Writing {len(raw_all)} raw rows to {OUT['H']} and {OUT['I']}")
    raw_all.to_csv(OUT["H"], header=False, index=False)
    raw_all.to_csv(OUT["I"], header=False, index=False)
    t_write_raw = log_timer("Write raw prices", t_write_raw)

if g_frame is not None:
    g_frame.to_csv(OUT["G"], header=False, index=False)

# Auxiliary B C D
pd.Series(universe_ids).to_csv(OUT["B"], header=False, index=False)

ts_sorted = sorted(all_ts)
ts_fmt = (
    pd.to_datetime(ts_sorted, utc=True).tz_convert("UTC").strftime(FMT).tolist()
)
pd.Series(ts_fmt).to_csv(OUT["D"], header=False, index=False)
with OUT["C"].open("w") as fhc:
    for sid in sorted(universe_ids):
        for t_str in ts_fmt:
            fhc.write(f"{sid},{t_str}\n")

for key in ["A", "H", "I"]:
    path = OUT[key]
    if path.exists():
        print(f"Created {path} ({path.stat().st_size} bytes)")
    else:
        print(f"Warning: expected {path} was not created")

log_timer("Export complete", t_total_start)
