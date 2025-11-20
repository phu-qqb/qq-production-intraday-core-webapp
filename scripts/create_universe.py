#!/usr/bin/env python
import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote_plus
import json
import boto3
from botocore.exceptions import ClientError
import argparse

# -------------------------------------------------------------------
# 0) REUTILISATION DE TA FONCTION DE CONNEXION EXACTE
# -------------------------------------------------------------------

def get_conn_from_secret(secret_name: str, region_name: str, default_driver: str) -> str:
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        raise RuntimeError(f"Failed to retrieve secret {secret_name}") from exc

    data = json.loads(resp.get("SecretString", ""))

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

# -------------------------------------------------------------------
# 1) LOAD INPUTS (paires.txt and F.txt)
# -------------------------------------------------------------------

def load_inputs():
    # Paires normales
    pairs = [line.strip() for line in open("paires.txt", "r", encoding="utf-8-sig")]

    # Lecture F.txt en mode bulletproof
    cleaned = []
    with open("F.txt", "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if "," not in line:
                raise RuntimeError(f"Ligne invalide dans F.txt : {line}")

            a, b = line.split(",", 1)
            cleaned.append((int(a.strip()), int(b.strip())))

    df_F = pd.DataFrame(cleaned, columns=["oldSub", "virtSecId"])
    return pairs, df_F



# -------------------------------------------------------------------
# 2) MAP VIRTUAL SECURITY IDs → REAL SECURITY IDs
# -------------------------------------------------------------------

def build_security_map(engine, pairs):
    virt_to_real = {}
    sql = sa.text("SELECT SecurityId FROM core.Security WHERE Symbol = :s")

    with engine.connect() as conn:
        for i, symbol in enumerate(pairs):
            virt = 100000 + i
            row = conn.execute(sql, {"s": symbol}).fetchone()
            if not row:
                raise RuntimeError(f"Missing symbol in core.Security: {symbol}")
            virt_to_real[virt] = int(row[0])

    return virt_to_real

# -------------------------------------------------------------------
# 3) CREATE UNIVERSE
# -------------------------------------------------------------------

def create_universe(engine, name="INFX1"):
    sql = sa.text("""
        INSERT INTO univ.Universe
            (Name, AssetTypeId, BaseCurrency, UniverseType, Source,
             SelectionSpecJson, Description, CreatedBy)
        OUTPUT INSERTED.UniverseId
        VALUES
            (:name, 1, 'USD', 'Backtest', 'Rule',
             '{"rule":"FX crosses"}', 'FX Universe Auto', 'philippe');
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, {"name": name}).fetchone()
        print("INSERT RESULT:", row)
        return row[0]

# -------------------------------------------------------------------
# 4) INSERT UNIVERSE MEMBERS
# -------------------------------------------------------------------

def insert_universe_members(engine, universe_id, pairs, virt_to_real):

    rows = []
    for idx, symbol in enumerate(pairs):
        virt = 100000 + idx
        real = virt_to_real[virt]
        rows.append({
            "SecurityId": real,
            "EffectiveFromUtc": "2022-01-01",
            "EffectiveToUtc": "9999-12-31",
            "Weight": None,
            "Rank": idx + 1,
            "Notes": None,
        })

    insert_sql = sa.text("""
        INSERT INTO univ.UniverseMember
        (UniverseId, SecurityId, EffectiveFromUtc, EffectiveToUtc, Weight, Rank, Notes, CreatedBy)
        VALUES (:UniverseId, :SecurityId, :EffectiveFromUtc, :EffectiveToUtc, :Weight, :Rank, :Notes, 'philippe')
    """)

    with engine.begin() as conn:
        conn.execute(
            insert_sql,
            [{"UniverseId": universe_id, **row} for row in rows]
        )

# -------------------------------------------------------------------
# 5) CREATE SUBUNIVERSE + MEMBERS (FULLY FIXED)
# -------------------------------------------------------------------

def create_subuniverses(engine, universe_id, df_F, virt_to_real):
    # Nettoyage dur et simple
    df_F["oldSub"] = df_F["oldSub"].astype(str).str.replace(r"[^\d]", "", regex=True).astype(int)
    df_F["virtSecId"] = df_F["virtSecId"].astype(str).str.replace(r"[^\d]", "", regex=True).astype(int)

    # Mapping: oldSub -> SubUniverseId (qu'on va remplir au fur et à mesure)
    sub_map: dict[int, int] = {}

    insert_sub_sql = sa.text("""
        INSERT INTO univ.SubUniverse
            (UniverseId, Name, SubUniverseType, SelectionSpecJson, Description, IsActive, CreatedBy)
        OUTPUT INSERTED.SubUniverseId
        VALUES (:UniverseId, :Name, 'Generic', NULL, NULL, 1, 'philippe');
    """)

    insert_member_sql = sa.text("""
        INSERT INTO univ.SubUniverseMember
            (SubUniverseId, SecurityId, EffectiveFromUtc, EffectiveToUtc,
             Weight, Rank, Notes, CreatedBy)
        VALUES (:SubUniverseId, :SecurityId, '2022-01-01', '9999-12-31',
                NULL, NULL, NULL, 'philippe');
    """)

    with engine.begin() as conn:
        for row in df_F.itertuples(index=False):
            old = int(row.oldSub)
            virt = int(row.virtSecId)

            # Si on n'a pas encore créé le SubUniverse pour cet oldSub -> on le crée
            if old not in sub_map:
                res = conn.execute(
                    insert_sub_sql,
                    {"UniverseId": universe_id, "Name": f"SUB_{old}"}
                )
                sub_id = int(res.fetchone()[0])
                sub_map[old] = sub_id

            # On insère le membre
            conn.execute(
                insert_member_sql,
                {
                    "SubUniverseId": sub_map[old],
                    "SecurityId": virt_to_real[virt],
                },
            )

    print(f"SubUniverses créés: {len(sub_map)}")


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

if __name__ == "__main__":

    secret = "qq-intraday-test-credentials"
    region = "eu-west-2"
    driver = "ODBC Driver 17 for SQL Server"

    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", required=True, help="Universe name")
    parser.add_argument("--timeframe", type=int, default=60)
    parser.add_argument("--secret-name", default="qq-intraday-test-credentials")
    parser.add_argument("--session", default="US")   # ou EU si tu préfères
    parser.add_argument("--start", default="2022-01-01")

    args = parser.parse_args()

    conn_str = get_conn_from_secret(secret, region, driver)
    engine = sa.create_engine(conn_str)

    # Inputs
    pairs, df_F = load_inputs()

    # Security mapping
    virt_to_real = build_security_map(engine, pairs)

    # Universe
    universe_id = create_universe(engine, args.universe)
    print(f"Created Universe {universe_id}")

    # Universe members
    insert_universe_members(engine, universe_id, pairs, virt_to_real)
    print("Universe members inserted.")

    # SubUniverses + members
    create_subuniverses(engine, universe_id, df_F, virt_to_real)
    print("SubUniverses and members inserted.")

    print("ALL DONE — FX UNIVERSE FULLY BUILT ✔")
