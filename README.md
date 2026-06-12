# Fabric MLV schema-mismatch repro

Standalone repro for [microsoft/dbt-fabricspark#216](https://github.com/microsoft/dbt-fabricspark/issues/216) — Fabric `CREATE OR REPLACE MATERIALIZED LAKE VIEW` rejects a column-set change via the High-Concurrency Livy endpoint, even though the same SQL is reported to succeed in a Fabric `%%sql` notebook cell.

No dbt, no notebook. Pure HTTP to `…/highConcurrencySessions/.../repls/.../statements`.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
az login
```

## Run

```bash
export WORKSPACE_ID=<workspace GUID>
export LAKEHOUSE_ID=<lakehouse GUID>
export LAKEHOUSE_NAME=<lakehouse name>   # must be schema-enabled
# optional: export SCHEMA=dbo
# optional: export AZURE_TOKEN=<bearer>  (skip az login, use a pre-minted token)

python repro.py
```

Or edit the constants at the top of `repro.py` directly.

## Expected output

Cells 1–6 succeed (drop, create source with CDF, insert, MLV v1, `SELECT *`). **Cell 7 fails** with:

```
[MLV_SCHEMA_MISMATCH] Unable to refresh materialized lake view due to schema mismatch.
Schema changes detected: Expected: root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- amount: integer (nullable = true)
, Found: root
 |-- id: integer (nullable = true)
 |-- amount: integer (nullable = true)
 |-- amount_doubled: integer (nullable = true)
. Revert the schema changes in the notebook or run the updated materialized lake view
definition with replace=true in the @fmlv decorator before lineage refresh.
```

The script then cleans up (drops MLV + source + HC session) and exits with code 1.

If cell 7 succeeds in your environment, the bug doesn't repro — please share what's different.
