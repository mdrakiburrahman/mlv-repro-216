#!/usr/bin/env python3
"""Repro: Fabric MLV CREATE OR REPLACE rejects schema change via HC Livy.

Mirrors microsoft/dbt-fabricspark#216 (comment 4695351801). No dbt, no
notebook — pure HTTP to Fabric's /highConcurrencySessions/.../statements
endpoint. The schema-changing CREATE OR REPLACE in cell 7 fails with
MLV_SCHEMA_MISMATCH, despite the same SQL succeeding in a Fabric %%sql cell.
"""
import json, os, sys, time
import requests

WORKSPACE_ID   = os.getenv("WORKSPACE_ID",   "<paste workspace GUID>")
LAKEHOUSE_ID   = os.getenv("LAKEHOUSE_ID",   "<paste lakehouse GUID>")
LAKEHOUSE_NAME = os.getenv("LAKEHOUSE_NAME", "<paste lakehouse name>")  # schema-enabled
SCHEMA         = os.getenv("SCHEMA",         "dbo")
SESSION_TIMEOUT_S = 1500
CLEANUP        = True

API = (f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
       f"/lakehouses/{LAKEHOUSE_ID}/livyapi/versions/2023-12-01/highConcurrencySessions")
AAD_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

FQ_SRC = f"`{LAKEHOUSE_NAME}`.`{SCHEMA}`.mlv_repro_source"
FQ_MLV = f"`{LAKEHOUSE_NAME}`.`{SCHEMA}`.mlv_repro"

CELLS = [
    ("drop mlv",       f"DROP MATERIALIZED LAKE VIEW IF EXISTS {FQ_MLV}"),
    ("drop source",    f"DROP TABLE IF EXISTS {FQ_SRC}"),
    ("create source",  f"CREATE TABLE {FQ_SRC} (id INT, name STRING, amount INT) "
                       f"USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)"),
    ("insert rows",    f"INSERT INTO {FQ_SRC} VALUES "
                       f"(1,'alice',100), (2,'bob',200), (3,'charlie',300)"),
    ("MLV v1 (id, name, amount)",
                       f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {FQ_MLV} AS "
                       f"SELECT id, name, amount FROM {FQ_SRC}"),
    ("select * from MLV",
                       f"SELECT * FROM {FQ_MLV} ORDER BY id"),
    ("MLV v2 (id, amount, amount_doubled) — EXPECTED TO FAIL",
                       f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {FQ_MLV} AS "
                       f"SELECT id, amount, amount * 2 AS amount_doubled FROM {FQ_SRC}"),
]


def token():
    if t := os.getenv("AZURE_TOKEN"):
        return t
    if not hasattr(token, "_t"):
        from azure.identity import AzureCliCredential
        token._t = AzureCliCredential().get_token(AAD_SCOPE).token
    return token._t


def headers():
    return {"Authorization": f"Bearer {token()}", "Content-Type": "application/json"}


def acquire():
    r = requests.post(API, headers=headers(),
                      data=json.dumps({"sessionTag": "mlv-repro", "name": "mlv-repro"}),
                      timeout=60)
    r.raise_for_status()
    hc = r.json()["id"]
    print(f"  hc={hc} — polling for Idle")
    deadline, last = time.time() + SESSION_TIMEOUT_S, None
    while time.time() < deadline:
        body = requests.get(f"{API}/{hc}", headers=headers(), timeout=60).json()
        state = body.get("state")
        if state != last:
            print(f"    state={state}"); last = state
        if state == "Idle":
            return hc, body["sessionId"], body["replId"]
        if state in ("Error", "Dead", "Killed"):
            raise RuntimeError(f"HC {hc} {state}: {body}")
        time.sleep(10)
    raise TimeoutError(f"HC {hc} not Idle in {SESSION_TIMEOUT_S}s")


def fire(sess, repl, code):
    base = f"{API}/{sess}/repls/{repl}/statements"
    r = requests.post(base, headers=headers(),
                      data=json.dumps({"code": code, "kind": "sql"}), timeout=60)
    r.raise_for_status()
    sid = r.json()["id"]
    while True:
        body = requests.get(f"{base}/{sid}", headers=headers(), timeout=60).json()
        if body.get("state") in ("available", "error", "cancelled"):
            return body.get("output") or {}
        time.sleep(2)


def main():
    for k, v in (("WORKSPACE_ID", WORKSPACE_ID),
                 ("LAKEHOUSE_ID", LAKEHOUSE_ID),
                 ("LAKEHOUSE_NAME", LAKEHOUSE_NAME)):
        if v.startswith("<paste"):
            sys.exit(f"set {k} (edit the script or export the env var)")

    print(f"workspace : {WORKSPACE_ID}")
    print(f"lakehouse : {LAKEHOUSE_NAME} ({LAKEHOUSE_ID})")
    print(f"schema    : {SCHEMA}\n")

    print("[setup] acquiring HC Livy session…")
    hc, sess, repl = acquire()
    print(f"[setup] ready session={sess} repl={repl}\n")

    failed = 0
    try:
        for label, code in CELLS:
            print(f"▶ {label}")
            out = fire(sess, repl, code)
            if out.get("status") == "ok":
                rows = (out.get("data") or {}).get("application/json", {}).get("data")
                if rows:
                    for row in rows: print(f"   → {row}")
                else:
                    print("   ✓ OK")
            else:
                failed += 1
                print(f"   ✗ {out.get('ename')}: {out.get('evalue')}")
            print()
    finally:
        if CLEANUP:
            print("[cleanup] dropping MLV + source + HC session")
            for code in (f"DROP MATERIALIZED LAKE VIEW IF EXISTS {FQ_MLV}",
                         f"DROP TABLE IF EXISTS {FQ_SRC}"):
                fire(sess, repl, code)
            try: requests.delete(f"{API}/{hc}", headers=headers(), timeout=30)
            except Exception: pass

    print("\nREPRO HIT" if failed else "\nno repro — all cells passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
