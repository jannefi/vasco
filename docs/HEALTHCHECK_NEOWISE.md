
# NEOWISE TAP Healthcheck

This document describes how to verify the state of async TAP jobs and their per-chunk outputs in the **delta** workflow.

## Script
- `scripts/healthcheck_tap_neowise.py` — scans `positions_chunk_*.csv` (new-only path) and classifies each chunk:
  - **NEW** — ready to submit
  - **IN_FLIGHT** — job URL found; `/phase` is neither COMPLETED nor ERROR
  - **PARTIAL** — RAW exists but `*_closest.csv` missing **or** phase=COMPLETED and `*_closest.csv` missing
  - **NEED_RESUBMIT** — phase ERROR or unreachable; `*_closest.csv` missing
  - **COMPLETED** — `*_closest.csv` exists and is non-empty

## Usage
```bash
python ./scripts/healthcheck_tap_neowise.py   --positions-dir ./data/local-cats/tmp/positions   --glob 'new/positions_chunk_*.csv'   --out-md  ./data/local-cats/tmp/healthcheck_neowise.md   --out-csv ./data/local-cats/tmp/healthcheck_neowise.csv
```

The Markdown summary provides **remediation commands** for PARTIAL and NEED_RESUBMIT states.

## When to run
- Before `make post15_async_chunks` to see what remains NEW
- After a TAP batch completes to confirm **COMPLETED** coverage
- If you suspect **lost** jobs or partial outputs

## Notes
- The checker prefers `curl` for `/phase` queries and falls back to Python's `urllib`.
- It never resubmits automatically; it only reports status and prints safe remediation commands.
- It works with your existing naming (`*_raw.csv`, `*_closest.csv`, `*_tap.meta.json`).
