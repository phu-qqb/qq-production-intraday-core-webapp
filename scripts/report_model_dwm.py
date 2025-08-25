#!/usr/bin/env python
"""Generate intraday model performance reports from SQL Server.

This script connects to SQL Server, runs the stored procedure
``model.Report_ModelDWM`` which returns seven result sets describing
intraday model performance, and produces CSV exports along with a PDF and
HTML report containing tables and charts.  Only pandas and matplotlib are
used for data handling and plotting.

The business rules applied upstream to the stored procedure (repeated
here for reference) are:
* PnL per bar is timestamped on the price timeline corresponding to the
  weight timeline shifted by two positions.
* Transaction cost is proportional to the turnover of the previous bar
  (|w_t - w_{t-1}|).
* NetPnL aggregates are computed as ``GrossPnL - Cost``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyodbc
from matplotlib.backends.backend_pdf import PdfPages


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

@dataclass
class ReportParams:
    conn_str: str
    model_id: int
    timeframe: int
    from_date: str | None
    to_date: str | None
    annualize_days: int
    top_n_pairs: int
    output_dir: str


RESULTSET_NAMES = [
    "daily_perf",
    "weekly_perf",
    "monthly_perf",
    "daily_by_pair",
    "weekly_by_pair",
    "monthly_by_pair",
    "risk_snapshot",
]


def get_conn_from_secret(secret_name: str, region_name: str, default_driver: str) -> str:
    """Return an ODBC connection string from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        raise RuntimeError(f"Failed to retrieve secret {secret_name}") from exc

    secret_str = resp.get("SecretString", "")
    data = json.loads(secret_str)

    if "conn" in data:
        return data["conn"]

    user = data.get("username", "")
    password = data.get("password", "")
    host = data.get("host")
    port = data.get("port", 1433)
    db = data.get("dbname") or data.get("database") or ""
    driver = data.get("driver", default_driver)
    return (
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={db};"
        f"UID={user};PWD={password};Encrypt=no"
    )


def fetch_report(params: ReportParams) -> Dict[str, pd.DataFrame]:
    """Run the stored procedure and return result sets as DataFrames."""
    query = (
        "EXEC model.Report_ModelDWM "
        "@ModelId = ?, @TimeframeMinute = ?, @FromDate = ?, "
        "@ToDate = ?, @AnnualizeDays = ?"
    )
    logging.info("Executing stored procedure model.Report_ModelDWM")
    with pyodbc.connect(params.conn_str) as conn:
        cur = conn.cursor()
        cur.execute(
            query,
            params.model_id,
            params.timeframe,
            params.from_date,
            params.to_date,
            params.annualize_days,
        )
        dfs: Dict[str, pd.DataFrame] = {}
        for name in RESULTSET_NAMES:
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame.from_records(rows, columns=cols)
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            dfs[name] = df
            if not cur.nextset():
                break
    # Convert date columns
    for key in ("daily_perf", "daily_by_pair"):
        if not dfs[key].empty:
            dfs[key]["DayDate"] = pd.to_datetime(dfs[key]["DayDate"])
    for key in ("weekly_perf", "weekly_by_pair"):
        if not dfs[key].empty:
            dfs[key]["WeekStart"] = pd.to_datetime(dfs[key]["WeekStart"])
    for key in ("monthly_perf", "monthly_by_pair"):
        if not dfs[key].empty:
            dfs[key]["MonthStart"] = pd.to_datetime(dfs[key]["MonthStart"])
    return dfs


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

CSV_MAPPING = {
    "daily_perf": "daily_perf.csv",
    "weekly_perf": "weekly_perf.csv",
    "monthly_perf": "monthly_perf.csv",
    "daily_by_pair": "daily_by_pair.csv",
    "weekly_by_pair": "weekly_by_pair.csv",
    "monthly_by_pair": "monthly_by_pair.csv",
    "risk_snapshot": "risk_snapshot.csv",
}


def export_csv(dfs: Dict[str, pd.DataFrame], out_dir: str) -> Dict[str, str]:
    """Export all datasets to CSV files."""
    paths: Dict[str, str] = {}
    for key, fname in CSV_MAPPING.items():
        path = os.path.join(out_dir, fname)
        df = dfs.get(key, pd.DataFrame())
        df.to_csv(path, index=False, float_format="%.6f")
        paths[key] = path
        logging.info("Wrote %s", path)
    return paths


# ---------------------------------------------------------------------------
# Figure construction
# ---------------------------------------------------------------------------

def _fig_equity_curve(daily: pd.DataFrame) -> plt.Figure:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    if daily.empty:
        ax1.text(0.5, 0.5, "No data", ha="center")
        ax2.axis("off")
        return fig
    net = daily.set_index("DayDate")["NetPnL"].fillna(0)
    cum_net = net.cumsum()
    running_max = cum_net.cummax()
    drawdown = cum_net - running_max
    ax1.plot(cum_net.index, cum_net.values, label="Cumulative NetPnL")
    ax1.axhline(0, color="black", linewidth=0.5)
    ax1.set_title("Equity Curve")
    ax1.set_ylabel("NetPnL")
    ax2.fill_between(
        drawdown.index,
        drawdown.values,
        0,
        where=drawdown.values < 0,
        color="red",
        alpha=0.3,
    )
    ax2.axhline(0, color="black", linewidth=0.5)
    ax2.set_ylabel("Drawdown")
    ax2.set_xlabel("Date")
    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)
    ax1.xaxis.set_major_locator(locator)
    ax1.xaxis.set_major_formatter(formatter)
    ax2.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_formatter(formatter)
    fig.tight_layout()
    return fig


def _fig_histogram(daily: pd.DataFrame, mean: float, var95: float) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    if daily.empty:
        ax.text(0.5, 0.5, "No data", ha="center")
        return fig
    net = daily["NetPnL"].fillna(0)
    ax.hist(net, bins=30)
    ax.axvline(mean, color="green", linestyle="--", label="Mean")
    ax.axvline(var95, color="red", linestyle="--", label="VaR95")
    ax.set_title("Distribution of Daily NetPnL")
    ax.legend()
    ax.set_xlabel("NetPnL")
    ax.set_ylabel("Frequency")
    fig.tight_layout()
    return fig


def _fig_rolling_sharpe(daily: pd.DataFrame, ann_days: int) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    if daily.empty:
        ax.text(0.5, 0.5, "No data", ha="center")
        return fig
    net = daily.set_index("DayDate")["NetPnL"].fillna(0)
    roll_mean = net.rolling(21).mean()
    roll_std = net.rolling(21).std()
    roll_sharpe = (roll_mean / roll_std) * np.sqrt(ann_days)
    roll_sharpe.replace([np.inf, -np.inf], np.nan, inplace=True)
    roll_sharpe.dropna(inplace=True)
    ax.plot(roll_sharpe.index, roll_sharpe.values)
    ax.set_title("21-day Rolling Sharpe")
    ax.set_xlabel("Date")
    ax.set_ylabel("Sharpe")
    locator = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    fig.tight_layout()
    return fig


def _fig_pair_attrib(daily_by_pair: pd.DataFrame, top_n: int) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 6))
    if daily_by_pair.empty:
        ax.text(0.5, 0.5, "No data", ha="center")
        return fig
    agg = daily_by_pair.groupby("PairCode")["NetPnL"].sum().sort_values()
    top = agg.tail(top_n)
    bottom = agg.head(top_n)
    combined = pd.concat([bottom, top])
    combined.plot(kind="barh", ax=ax)
    ax.set_title(f"Cumulative NetPnL by Pair (Top/Bottom {top_n})")
    ax.set_xlabel("NetPnL")
    fig.tight_layout()
    return fig


def _fig_heatmap(weekly_by_pair: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 6))
    if weekly_by_pair.empty:
        ax.text(0.5, 0.5, "No data", ha="center")
        return fig
    pivot = weekly_by_pair.pivot_table(
        index="WeekStart", columns="PairCode", values="NetPnL", aggfunc="sum"
    )
    if pivot.empty:
        ax.text(0.5, 0.5, "No data", ha="center")
        return fig
    contrib = pivot.abs().sum().sort_values(ascending=False)
    top_pairs = contrib.index[:12]
    pivot = pivot[top_pairs]
    im = ax.imshow(pivot.T.values, aspect="auto")
    ax.set_yticks(range(len(pivot.columns)))
    ax.set_yticklabels(pivot.columns)
    ax.set_xticks(range(len(pivot.index)))
    ax.set_xticklabels(pivot.index.strftime("%Y-%m-%d"), rotation=90)
    ax.set_title("Weekly NetPnL Heatmap by Pair")
    fig.colorbar(im, ax=ax)
    fig.tight_layout()
    return fig


def _fig_bar(df: pd.DataFrame, date_col: str, title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    if df.empty:
        ax.text(0.5, 0.5, "No data", ha="center")
        return fig
    ax.bar(df[date_col], df["NetPnL"])
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("NetPnL")
    locator = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    fig.tight_layout()
    return fig


def build_figures(dfs: Dict[str, pd.DataFrame], params: ReportParams) -> Dict[str, plt.Figure]:
    risk = dfs.get("risk_snapshot", pd.DataFrame())
    mean_daily = float(risk.get("MeanDaily", pd.Series([0])).iloc[0]) if not risk.empty else 0.0
    var95 = float(risk.get("VaR95_Daily", pd.Series([0])).iloc[0]) if not risk.empty else 0.0
    figures = {
        "equity_curve": _fig_equity_curve(dfs.get("daily_perf", pd.DataFrame())),
        "histogram": _fig_histogram(dfs.get("daily_perf", pd.DataFrame()), mean_daily, var95),
        "rolling_sharpe": _fig_rolling_sharpe(dfs.get("daily_perf", pd.DataFrame()), params.annualize_days),
        "pair_attrib": _fig_pair_attrib(dfs.get("daily_by_pair", pd.DataFrame()), params.top_n_pairs),
        "heatmap": _fig_heatmap(dfs.get("weekly_by_pair", pd.DataFrame())),
        "weekly_bar": _fig_bar(dfs.get("weekly_perf", pd.DataFrame()), "WeekStart", "Weekly NetPnL"),
        "monthly_bar": _fig_bar(dfs.get("monthly_perf", pd.DataFrame()), "MonthStart", "Monthly NetPnL"),
    }
    return figures


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def _summary_text(dfs: Dict[str, pd.DataFrame], params: ReportParams) -> str:
    daily = dfs.get("daily_perf", pd.DataFrame())
    risk = dfs.get("risk_snapshot", pd.DataFrame())
    start = daily["DayDate"].min() if not daily.empty else None
    end = daily["DayDate"].max() if not daily.empty else None
    totals = daily[["GrossPnL", "Cost", "NetPnL", "TurnoverAbsSum"]].sum()
    r = risk.iloc[0] if not risk.empty else pd.Series(dtype=float)
    text = [
        f"Period: {start.date() if start is not None else 'NA'} to {end.date() if end is not None else 'NA'}",
        f"ModelId: {params.model_id}  TF: {params.timeframe}",
        f"NumDays: {int(r.get('NumDays', 0))}",
        f"AnnMean: {r.get('AnnMean', 0):.6f}",
        f"AnnVol: {r.get('AnnVol', 0):.6f}",
        f"Sharpe: {r.get('Sharpe', 0):.2f}",
        f"ProfitFactorDaily: {r.get('ProfitFactorDaily', 0):.2f}",
        f"HitRatioDaily: {r.get('HitRatioDaily', 0):.2f}",
        f"MaxDrawdown: {r.get('MaxDrawdown', 0):.6f} ({r.get('MaxDDDate', '')})",
        f"VaR95_Daily: {r.get('VaR95_Daily', 0):.6f}",
        "",
        "Totals over period:",
        f"  GrossPnL: {totals.get('GrossPnL', 0):.6f}",
        f"  Cost: {totals.get('Cost', 0):.6f}",
        f"  NetPnL: {totals.get('NetPnL', 0):.6f}",
        f"  TurnoverAbsSum: {totals.get('TurnoverAbsSum', 0):.6f}",
        "",
        "Business rules:",
        "  - PnL per bar timestamped on price timeline shifted by 2 from weights.",
        "  - Cost proportional to prior bar turnover (|w_t - w_{t-1}|).",
        "  - NetPnL = GrossPnL - Cost.",
    ]
    return "\n".join(text)


def _fig_table(df: pd.DataFrame, title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(11, 8))
    ax.set_axis_off()
    ax.set_title(title)
    if df.empty:
        ax.text(0.5, 0.5, "No data", ha="center")
        return fig
    table = ax.table(
        cellText=df.values,
        colLabels=list(df.columns),
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.5)
    fig.tight_layout()
    return fig


def _top_bottom_tables(daily_by_pair: pd.DataFrame, top_n: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if daily_by_pair.empty:
        return pd.DataFrame(columns=["PairCode", "NetPnL", "%"], index=[]), pd.DataFrame(columns=["PairCode", "NetPnL", "%"], index=[])
    agg = daily_by_pair.groupby("PairCode")["NetPnL"].sum()
    total = agg.sum()
    agg_pct = agg / total * 100 if total != 0 else agg * 0
    df = pd.DataFrame({"PairCode": agg.index, "NetPnL": agg.values, "%": agg_pct.values})
    df.sort_values("NetPnL", inplace=True)
    bottom = df.head(top_n)
    top = df.tail(top_n).iloc[::-1]
    return top, bottom


def render_pdf_html(
    dfs: Dict[str, pd.DataFrame],
    figures: Dict[str, plt.Figure],
    params: ReportParams,
    fname_base: str,
) -> Tuple[str, str]:
    pdf_path = os.path.join(params.output_dir, f"{fname_base}.pdf")
    html_path = os.path.join(params.output_dir, f"{fname_base}.html")

    # Save figures to PNG for embedding in HTML
    img_paths: Dict[str, str] = {}
    for key, fig in figures.items():
        img_file = os.path.join(params.output_dir, f"{fname_base}_{key}.png")
        fig.savefig(img_file, bbox_inches="tight")
        img_paths[key] = os.path.basename(img_file)

    summary_text = _summary_text(dfs, params)

    with PdfPages(pdf_path) as pdf:
        fig = plt.figure(figsize=(8.27, 11.69))
        ax = fig.add_subplot(111)
        ax.axis("off")
        ax.text(0, 1, summary_text, ha="left", va="top", family="monospace")
        pdf.savefig(fig)
        plt.close(fig)

        pdf.savefig(figures["equity_curve"])
        pdf.savefig(figures["histogram"])
        pdf.savefig(figures["rolling_sharpe"])
        pdf.savefig(figures["pair_attrib"])
        pdf.savefig(figures["heatmap"])
        pdf.savefig(figures["weekly_bar"])
        pdf.savefig(figures["monthly_bar"])

        daily_table = dfs.get("daily_perf", pd.DataFrame())[
            [
                "DayDate",
                "GrossPnL",
                "Cost",
                "NetPnL",
                "NBars",
                "HitRatioBars",
                "TurnoverAbsSum",
            ]
        ]
        weekly_table = dfs.get("weekly_perf", pd.DataFrame())[
            ["WeekStart", "GrossPnL", "Cost", "NetPnL", "NBars"]
        ]
        monthly_table = dfs.get("monthly_perf", pd.DataFrame())[
            ["MonthStart", "GrossPnL", "Cost", "NetPnL", "NBars"]
        ]
        top_pairs, bottom_pairs = _top_bottom_tables(
            dfs.get("daily_by_pair", pd.DataFrame()), params.top_n_pairs
        )
        pdf.savefig(_fig_table(daily_table, "Daily Performance"))
        pdf.savefig(_fig_table(weekly_table, "Weekly Performance"))
        pdf.savefig(_fig_table(monthly_table, "Monthly Performance"))
        pdf.savefig(_fig_table(top_pairs, "Top Pairs"))
        pdf.savefig(_fig_table(bottom_pairs, "Bottom Pairs"))

    # HTML report
    html: List[str] = [
        "<html><head><meta charset='utf-8'><title>Report</title></head><body>",
        "<pre>" + summary_text + "</pre>",
    ]
    for key in [
        "equity_curve",
        "histogram",
        "rolling_sharpe",
        "pair_attrib",
        "heatmap",
        "weekly_bar",
        "monthly_bar",
    ]:
        html.append(f"<h3>{key.replace('_', ' ').title()}</h3>")
        html.append(f"<img src='{img_paths[key]}' alt='{key}'>")

    html.append("<h3>Daily Performance</h3>")
    html.append(daily_table.to_html(index=False))
    html.append("<h3>Weekly Performance</h3>")
    html.append(weekly_table.to_html(index=False))
    html.append("<h3>Monthly Performance</h3>")
    html.append(monthly_table.to_html(index=False))
    html.append("<h3>Top Pairs</h3>")
    html.append(top_pairs.to_html(index=False))
    html.append("<h3>Bottom Pairs</h3>")
    html.append(bottom_pairs.to_html(index=False))

    html.append("</body></html>")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html))
    logging.info("Wrote %s", pdf_path)
    logging.info("Wrote %s", html_path)
    return pdf_path, html_path


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def write_manifest(paths: Dict[str, str], out_pdf: str, out_html: str, dfs: Dict[str, pd.DataFrame], params: ReportParams) -> None:
    manifest = {
        "outputs": {
            **paths,
            "pdf": out_pdf,
            "html": out_html,
        },
        "model_id": params.model_id,
        "timeframe": params.timeframe,
    }
    risk = dfs.get("risk_snapshot", pd.DataFrame())
    if not risk.empty:
        manifest["risk_snapshot"] = risk.iloc[0].to_dict()
    path = os.path.join(params.output_dir, "manifest.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    logging.info("Wrote %s", path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> ReportParams:
    parser = argparse.ArgumentParser(description="Generate Model DWM report")
    parser.add_argument(
        "--conn-string",
        help="ODBC connection string (overrides AWS secret if provided)",
    )
    parser.add_argument(
        "--secret-name",
        default="qq-intraday-credentials",
        help="AWS Secrets Manager name containing DB credentials",
    )
    parser.add_argument(
        "--region",
        default="eu-west-2",
        help="AWS region where the secret is stored",
    )
    parser.add_argument(
        "--driver",
        default="ODBC Driver 17 for SQL Server",
        help="ODBC driver name to use when connecting via pyodbc",
    )
    parser.add_argument("--model-id", type=int, required=True)
    parser.add_argument("--timeframe", type=int, required=True)
    parser.add_argument("--from-date", default=None, help="YYYY-MM-DD or null")
    parser.add_argument("--to-date", default=None, help="YYYY-MM-DD or null")
    parser.add_argument("--annualize-days", type=int, default=252)
    parser.add_argument("--top-n-pairs", type=int, default=10)
    parser.add_argument("--output-dir", default="output", help="Directory for outputs")
    args = parser.parse_args()
    return ReportParams(
        conn_str=args.conn_string
        or get_conn_from_secret(args.secret_name, args.region, args.driver),
        model_id=args.model_id,
        timeframe=args.timeframe,
        from_date=args.from_date,
        to_date=args.to_date,
        annualize_days=args.annualize_days,
        top_n_pairs=args.top_n_pairs,
        output_dir=args.output_dir,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    params = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    os.makedirs(params.output_dir, exist_ok=True)

    dfs = fetch_report(params)
    paths = export_csv(dfs, params.output_dir)
    figures = build_figures(dfs, params)
    fname_base = f"Report_Model_{params.model_id}_{params.timeframe}_{params.from_date or 'NA'}_{params.to_date or 'NA'}"
    pdf_path, html_path = render_pdf_html(dfs, figures, params, fname_base)
    write_manifest(paths, pdf_path, html_path, dfs, params)


if __name__ == "__main__":  # pragma: no cover
    main()
