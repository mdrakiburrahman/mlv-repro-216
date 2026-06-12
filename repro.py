#!/usr/bin/env python3
"""Repro for microsoft/dbt-fabricspark#216: Fabric MLV CREATE OR REPLACE
rejects schema changes via HC Livy with MLV_SCHEMA_MISMATCH."""
import json, os, sys, time, requests
from azure.identity import AzureCliCredential

WS   = os.environ["WORKSPACE_ID"]
LH   = os.environ["LAKEHOUSE_ID"]
NAME = os.environ["LAKEHOUSE_NAME"]      # schema-enabled lakehouse
SCH  = os.getenv("SCHEMA", "dbo")

API = (f"https://api.fabric.microsoft.com/v1/workspaces/{WS}"
       f"/lakehouses/{LH}/livyapi/versions/2023-12-01/highConcurrencySessions")
TOK = os.getenv("AZURE_TOKEN") or AzureCliCredential().get_token(
    "https://analysis.windows.net/powerbi/api/.default").token
H = {"Authorization": f"Bearer {TOK}", "Content-Type": "application/json"}

SRC = f"`{NAME}`.`{SCH}`.mlv_repro_source"
MLV = f"`{NAME}`.`{SCH}`.mlv_repro"
CELLS = [
    f"DROP MATERIALIZED LAKE VIEW IF EXISTS {MLV}",
    f"DROP TABLE IF EXISTS {SRC}",
    f"CREATE TABLE {SRC} (id INT, name STRING, amount INT) USING DELTA "
    f"TBLPROPERTIES (delta.enableChangeDataFeed = true)",
    f"INSERT INTO {SRC} VALUES (1,'alice',100),(2,'bob',200),(3,'charlie',300)",
    f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {MLV} AS SELECT id, name, amount FROM {SRC}",
    f"SELECT * FROM {MLV} ORDER BY id",
    f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {MLV} AS "
    f"SELECT id, amount, amount * 2 AS amount_doubled FROM {SRC}  -- EXPECTED FAIL",
]

def post(url, body): return requests.post(url, headers=H, data=json.dumps(body), timeout=60)
def get(url):        return requests.get(url, headers=H, timeout=60).json()

hc = post(API, {"sessionTag": "mlv-repro", "name": "mlv-repro"}).json()["id"]
print(f"hc={hc} waiting for Idle…")
for _ in range(150):
    b = get(f"{API}/{hc}")
    if b["state"] == "Idle": break
    if b["state"] in ("Error", "Dead", "Killed"): sys.exit(f"{b['state']}: {b}")
    time.sleep(10)
else: sys.exit("session never reached Idle")
sess, repl = b["sessionId"], b["replId"]
stmts = f"{API}/{sess}/repls/{repl}/statements"

def fire(code):
    sid = post(stmts, {"code": code, "kind": "sql"}).json()["id"]
    while (o := get(f"{stmts}/{sid}"))["state"] not in ("available", "error", "cancelled"):
        time.sleep(2)
    return o.get("output") or {}

failed = 0
try:
    for i, code in enumerate(CELLS, 1):
        print(f"\n▶ cell {i}: {code[:70]}{'…' if len(code) > 70 else ''}")
        out = fire(code)
        if out.get("status") == "ok":
            rows = (out.get("data") or {}).get("application/json", {}).get("data") or []
            for r in rows: print(f"   {r}")
            if not rows: print("   ok")
        else:
            failed += 1
            print(f"   ✗ {out.get('ename')}: {out.get('evalue')}")
finally:
    for code in (f"DROP MATERIALIZED LAKE VIEW IF EXISTS {MLV}", f"DROP TABLE IF EXISTS {SRC}"):
        try: fire(code)
        except Exception: pass
    try: requests.delete(f"{API}/{hc}", headers=H, timeout=30)
    except Exception: pass

print("\nREPRO HIT" if failed else "\nno repro")
sys.exit(1 if failed else 0)
