"""Run the DuckDB pipeline on the downloaded TLC files, twice, to show it is incremental.

    python scripts/run_pipeline.py            # uses data/raw/yellow_tripdata_*.parquet
"""
from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lakehouse.duck import Lake  # noqa: E402

raw = ROOT / "data" / "raw"
files = sorted(raw.glob("yellow_tripdata_*.parquet"))
lake_dir = ROOT / "data" / "lake"
shutil.rmtree(lake_dir, ignore_errors=True)

lake = Lake(lake_dir, raw / "taxi_zone_lookup.csv")
t0 = time.perf_counter()
first = lake.ingest(files[:-1])
t1 = time.perf_counter()
second = lake.ingest(files)            # only the new month is processed
t2 = time.perf_counter()
third = lake.ingest(files)             # nothing to do
t3 = time.perf_counter()

rows = sum(r.get("rows", 0) for r in first + second)
summary = {
    "files": [f.name for f in files],
    "rows_total": rows,
    "run1_seconds": round(t1 - t0, 2), "run2_seconds": round(t2 - t1, 2), "run3_seconds": round(t3 - t2, 2),
    "run2_status": {r["file"]: r["status"] for r in second},
    "rows_per_second_run1": int(sum(r.get("rows", 0) for r in first) / (t1 - t0)),
    "per_month": {k: {kk: v[kk] for kk in ("rows", "silver_rows", "rejected")} for k, v in lake.manifest.items()},
}
print(json.dumps(summary, indent=1))
out = ROOT / "results"
out.mkdir(exist_ok=True)
(out / "run_summary.json").write_text(json.dumps(summary, indent=1))
for name in ["daily_borough", "hourly_demand", "tips_by_payment", "top_routes"]:
    df = lake.gold(name)
    if name == "top_routes":
        df = df.sort_values("trips", ascending=False).head(15)
    df.to_csv(out / f"gold_{name}.csv", index=False)
print(lake.gold("tips_by_payment").to_string())
