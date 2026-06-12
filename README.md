# mlv-repro-216

Standalone repro for [microsoft/dbt-fabricspark#216](https://github.com/microsoft/dbt-fabricspark/issues/216) — Fabric `CREATE OR REPLACE MATERIALIZED LAKE VIEW` rejects a schema change via the HC Livy endpoint with `MLV_SCHEMA_MISMATCH`, despite the same SQL succeeding in a Fabric `%%sql` notebook cell.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
az login
WORKSPACE_ID=<ws-guid> LAKEHOUSE_ID=<lh-guid> LAKEHOUSE_NAME=<lh-name> python repro.py
```

Lakehouse must be schema-enabled. Cells 1–6 succeed, cell 7 fails with `MLV_SCHEMA_MISMATCH`. Cleanup runs in `finally`.
