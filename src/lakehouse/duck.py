"""Medallion pipeline on DuckDB: bronze (raw files + lineage) -> silver (clean, deduplicated,
partitioned by month) -> gold (KPI tables), with a quarantine table and an incremental manifest.

Layout under `lake/`:
  bronze/month=YYYY-MM/data.parquet      raw rows + source_file + ingested_at
  silver/month=YYYY-MM/data.parquet      rows that pass every rule, deduplicated
  quarantine/month=YYYY-MM/data.parquet  rejected rows + reject_reason
  gold/<table>.parquet                   recomputed from silver
  _manifest.json                         processed files with row counts and checksums
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from lakehouse import sql

MONTH_RE = re.compile(r"(\d{4})-(\d{2})")


def month_of(path: Path) -> str:
    m = MONTH_RE.search(path.name)
    if not m:
        raise ValueError(f"no YYYY-MM in file name {path.name}")
    return f"{m.group(1)}-{m.group(2)}"


def month_bounds(month: str) -> tuple[str, str]:
    y, m = map(int, month.split("-"))
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y:04d}-{m:02d}-01 00:00:00", f"{ny:04d}-{nm:02d}-01 00:00:00"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Lake:
    def __init__(self, root: Path, zones_csv: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect()
        self.con.execute(f"CREATE OR REPLACE VIEW zones AS SELECT * FROM read_csv_auto('{_p(zones_csv)}')")
        self.manifest_path = self.root / "_manifest.json"
        self.manifest = json.loads(self.manifest_path.read_text()) if self.manifest_path.exists() else {}

    def _part(self, layer: str, month: str) -> Path:
        d = self.root / layer / f"month={month}"
        d.mkdir(parents=True, exist_ok=True)
        return d / "data.parquet"

    # -- ingestion -------------------------------------------------------------------------------
    def ingest(self, files: list[Path]) -> list[dict]:
        """Process new or changed files only; re-running on the same files is a no-op."""
        report = []
        for f in sorted(files):
            digest = sha256(f)
            if self.manifest.get(f.name, {}).get("sha256") == digest:
                report.append({"file": f.name, "status": "skipped (already processed)"})
                continue
            report.append(self._process(f, digest))
        if any(r["status"] == "processed" for r in report):
            self.build_gold()
        self.manifest_path.write_text(json.dumps(self.manifest, indent=1))
        return report

    def _process(self, f: Path, digest: str) -> dict:
        t0 = time.perf_counter()
        month = month_of(f)
        start, end = month_bounds(month)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        bronze = self._part("bronze", month)
        self.con.execute(f"""COPY (SELECT *, '{f.name}' AS source_file, TIMESTAMP '{now}' AS ingested_at
                                   FROM read_parquet('{_p(f)}')) TO '{_p(bronze)}' (FORMAT parquet)""")
        self.con.execute(f"CREATE OR REPLACE VIEW bronze AS SELECT * FROM read_parquet('{_p(bronze)}')")
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE tagged AS {sql.tagged('duckdb', start, end)}")
        self.con.execute(f"COPY (SELECT {sql.SILVER_COLS} FROM tagged WHERE reject_reason IS NULL) "
                         f"TO '{_p(self._part('silver', month))}' (FORMAT parquet)")
        self.con.execute(f"COPY (SELECT * EXCLUDE (dup_rank) FROM tagged WHERE reject_reason IS NOT NULL) "
                         f"TO '{_p(self._part('quarantine', month))}' (FORMAT parquet)")
        reasons = dict(self.con.execute(
            "SELECT coalesce(reject_reason, 'ok'), COUNT(*) FROM tagged GROUP BY 1").fetchall())
        rows = sum(reasons.values())
        entry = {"month": month, "sha256": digest, "rows": rows, "silver_rows": reasons.get("ok", 0),
                 "rejected": {k: v for k, v in sorted(reasons.items()) if k != "ok"},
                 "seconds": round(time.perf_counter() - t0, 2), "processed_at": now}
        self.manifest[f.name] = entry
        return {"file": f.name, "status": "processed", **entry}

    # -- gold --------------------------------------------------------------------------------------
    def build_gold(self) -> None:
        self.con.execute(f"CREATE OR REPLACE VIEW silver AS SELECT * FROM "
                         f"read_parquet('{_p(self.root / 'silver')}/*/data.parquet')")
        gold = self.root / "gold"
        gold.mkdir(exist_ok=True)
        for name, q in sql.gold("duckdb").items():
            self.con.execute(f"COPY ({q}) TO '{_p(gold / (name + '.parquet'))}' (FORMAT parquet)")

    def gold(self, name: str):
        order = ", ".join(sql.GOLD_ORDER[name])
        return self.con.execute(
            f"SELECT * FROM read_parquet('{_p(self.root / 'gold' / (name + '.parquet'))}') ORDER BY {order}").df()

    def query(self, q: str):
        return self.con.execute(q).df()


def _p(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")
