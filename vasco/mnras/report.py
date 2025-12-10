from __future__ import annotations
from typing import Dict
from pathlib import Path
import json


def write_summary(
    run_dir: str, buckets: Dict[str, int], md_path: str, json_path: str
) -> None:
    run = Path(run_dir)
    # JSON
    jp = run / json_path
    jp.write_text(json.dumps(buckets, indent=2), encoding="utf-8")
    # Markdown
    mp = run / md_path
    lines = [
        "# MNRAS reproduction summary",
        "",
        "| Metric | Count |",
        "|---|---:|",
    ]
    for k, v in buckets.items():
        lines.append(f"| {k.replace('_',' ')} | {v} |")
    mp.write_text("\n".join(lines) + "\n", encoding="utf-8")
