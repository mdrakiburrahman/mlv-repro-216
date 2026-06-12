# mlv-repro-216

Standalone repro for [microsoft/dbt-fabricspark#216](https://github.com/microsoft/dbt-fabricspark/issues/216) — Fabric `CREATE OR REPLACE MATERIALIZED LAKE VIEW` rejects a schema change via the HC Livy endpoint with `MLV_SCHEMA_MISMATCH` because the analyzer's `validateReplaceMaterializedLakeView` treats non-notebook sessions as a DAG run. The SQL equivalent of `@fmlv(replace=True)` is a single Spark conf — `SET trident.artifact.type = SynapseNotebook` — that flips the gate (`MaterializedLakeViewAnalyzerBaseV2.isMLVCreateOrReplaceFromNotebook`).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
az login
WORKSPACE_ID=<ws-guid> LAKEHOUSE_ID=<lh-guid> LAKEHOUSE_NAME=<lh-name> python repro.py
```

Lakehouse must be schema-enabled. Cell 7 fails with `MLV_SCHEMA_MISMATCH`; cell 8 sets `trident.artifact.type = SynapseNotebook`; cells 9–10 then succeed with the new schema. Exit 0 iff every cell matches its expected outcome. Cleanup runs in `finally`.

