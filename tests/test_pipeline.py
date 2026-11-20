"""Pipeline tests on a small fixture where every bad row is known in advance."""
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pytest

from lakehouse import sql
from lakehouse.duck import Lake, month_bounds

BOROUGHS = ["Manhattan", "Queens", "Brooklyn"]


def make_month(path, month_start: datetime, n_good: int = 60):
    rows = []
    for i in range(n_good):
        pu = month_start + timedelta(hours=i * 5, minutes=i % 7)
        rows.append(dict(VendorID=1 + i % 2, tpep_pickup_datetime=pu,
                         tpep_dropoff_datetime=pu + timedelta(minutes=10 + i % 20),
                         passenger_count=1, trip_distance=1.0 + (i % 9) * 0.5, RatecodeID=1,
                         store_and_fwd_flag="N", PULocationID=1 + i % 6, DOLocationID=1 + (i * 3) % 6,
                         payment_type=1 + i % 2, fare_amount=10.0 + i % 5, extra=0.0, mta_tax=0.5,
                         tip_amount=(2.0 if i % 2 == 0 else 0.0), tolls_amount=0.0, improvement_surcharge=1.0,
                         total_amount=14.0 + i % 5, congestion_surcharge=2.5, Airport_fee=0.0))
    base = rows[0]
    bad = [
        ({"tpep_pickup_datetime": month_start - timedelta(days=40),
          "tpep_dropoff_datetime": month_start - timedelta(days=40) + timedelta(minutes=5)}, "pickup_in_month"),
        ({"tpep_dropoff_datetime": base["tpep_pickup_datetime"] - timedelta(minutes=1)}, "dropoff_after_pickup"),
        ({"tpep_dropoff_datetime": base["tpep_pickup_datetime"] + timedelta(hours=9)}, "duration_le_6h"),
        ({"trip_distance": 0.0}, "distance_in_range"),
        ({"fare_amount": -5.0}, "amounts_non_negative"),
        ({"PULocationID": 264}, "known_zones"),
    ]
    expected = {}
    for k, (patch, reason) in enumerate(bad):
        r = dict(base, **patch)
        r["total_amount"] = 100.0 + k          # distinct from the good rows
        rows.append(r)
        expected[reason] = 1
    rows.append(dict(rows[3]))                 # exact duplicate
    expected["duplicate"] = 1
    df = pd.DataFrame(rows)
    con = duckdb.connect()
    con.register("df", df)
    con.execute(f"COPY (SELECT * FROM df) TO '{str(path).replace(chr(92), '/')}' (FORMAT parquet)")
    return n_good, expected


@pytest.fixture()
def fixture(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    zones = raw / "zones.csv"
    zones.write_text("LocationID,Borough,Zone,service_zone\n" +
                     "\n".join(f"{i},{BOROUGHS[i % 3]},Zone {i},Yellow" for i in range(1, 264)) + "\n")
    meta = {}
    for m in (1, 2):
        f = raw / f"yellow_tripdata_2024-{m:02d}.parquet"
        meta[f.name] = make_month(f, datetime(2024, m, 1))
    return raw, zones, meta


def test_month_bounds():
    assert month_bounds("2024-12") == ("2024-12-01 00:00:00", "2025-01-01 00:00:00")


def test_quality_rules_and_quarantine(tmp_path, fixture):
    raw, zones, meta = fixture
    lake = Lake(tmp_path / "lake", zones)
    report = lake.ingest(sorted(raw.glob("*.parquet")))
    for r in report:
        n_good, expected = meta[r["file"]]
        assert r["silver_rows"] == n_good
        assert r["rejected"] == expected
    q = lake.query(f"SELECT reject_reason FROM read_parquet('{str(tmp_path / 'lake').replace(chr(92), '/')}"
                   f"/quarantine/*/data.parquet')")
    assert len(q) == 2 * 7


def test_incremental_and_idempotent(tmp_path, fixture):
    raw, zones, _ = fixture
    files = sorted(raw.glob("*.parquet"))
    lake = Lake(tmp_path / "lake", zones)
    lake.ingest(files[:1])
    r2 = lake.ingest(files)
    assert [r["status"] for r in r2] == ["skipped (already processed)", "processed"]
    before = lake.gold("daily_borough")
    lake2 = Lake(tmp_path / "lake", zones)                         # fresh process, same manifest
    assert all(r["status"].startswith("skipped") for r in lake2.ingest(files))
    pd.testing.assert_frame_equal(before, lake2.gold("daily_borough"))
    assert before["trips"].sum() == 120


def test_gold_values(tmp_path, fixture):
    raw, zones, _ = fixture
    lake = Lake(tmp_path / "lake", zones)
    lake.ingest(sorted(raw.glob("*.parquet")))
    tips = lake.gold("tips_by_payment").set_index("payment_type")
    assert tips.loc[1, "share_tipped"] == 1.0 and tips.loc[2, "share_tipped"] == 0.0
    assert lake.gold("hourly_demand")["trips"].sum() == 120
    assert set(lake.gold("daily_borough")["borough"]) <= set(BOROUGHS)


def test_spark_matches_duckdb(tmp_path, fixture):
    pytest.importorskip("pyspark")
    from lakehouse import spark as sp

    raw, zones, meta = fixture
    files = sorted(raw.glob("*.parquet"))
    lake = Lake(tmp_path / "lake", zones)
    lake.ingest(files)
    s = sp.session()
    report = sp.run(s, files, zones, tmp_path / "spark")
    for f in files:
        n_good, expected = meta[f.name]
        got = report[f.name[16:23]]
        assert got.pop("ok") == n_good and got == expected
    for name in sql.GOLD:
        d = lake.gold(name).reset_index(drop=True)
        k = sp.gold(s, tmp_path / "spark", name).reset_index(drop=True)
        k.columns = d.columns
        if "day" in d:
            d["day"], k["day"] = pd.to_datetime(d["day"]), pd.to_datetime(k["day"])
        pd.testing.assert_frame_equal(d, k, check_dtype=False, check_exact=False, rtol=1e-6)
