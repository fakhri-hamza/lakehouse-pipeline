# lakehouse-pipeline

![CI](https://github.com/fakhri-hamza/lakehouse-pipeline/actions/workflows/ci.yml/badge.svg) ![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

A **medallion (bronze / silver / gold) data pipeline** on 9.5 million real NYC taxi trips (TLC yellow, Jan–Mar 2024), with:

- **one SQL source, two engines**: the quality rules, deduplication and KPI queries are written once (`src/lakehouse/sql.py`) and run on **DuckDB** (local) and **Apache Spark / PySpark** (CI). A test runs both on the same fixture and checks that every gold table matches row by row;
- **data quality with a quarantine**: each rejected row keeps the first rule it broke, so bad data is counted and inspectable instead of silently dropped;
- **incremental, idempotent loads**: a manifest records each processed file with its SHA-256; re-running only processes new or changed files, and each month partition is overwritten as a whole, so a re-run never duplicates rows.

## Layers

| Layer | Content |
|---|---|
| bronze | raw rows + `source_file`, `ingested_at` (lineage), one partition per month |
| silver | rows passing every rule, deduplicated, with `duration_s` |
| quarantine | rejected rows + `reject_reason` |
| gold | `daily_borough`, `hourly_demand`, `tips_by_payment`, `top_routes` |

## Results on Jan–Mar 2024 (DuckDB, laptop)

| | |
|---|---|
| rows | 9,554,778 |
| first load (Jan + Feb, 5.97 M rows) | 10.8 s, about 555k rows/s end to end |
| second run (adds March only) | 7.4 s, Jan and Feb skipped |
| third run (nothing new) | 0.24 s |
| kept in silver | 9,121,417 (95.5 %) |

Rejected rows, by first broken rule:

| rule | Jan | Feb | Mar |
|---|---|---|---|
| distance_in_range (0 or > 200 miles) | 59,622 | 67,492 | 86,298 |
| amounts_non_negative | 34,138 | 37,451 | 54,680 |
| known_zones (264/265 = unknown / outside NYC) | 28,219 | 25,826 | 31,343 |
| duration_le_6h | 1,772 | 1,736 | 1,926 |
| dropoff_after_pickup | 870 | 803 | 1,128 |
| pickup_in_month (e.g. a 2002 timestamp) | 18 | 15 | 23 |
| duplicate | 0 | 1 | 0 |

A data caveat the gold tables make visible: card trips are tipped 95 % of the time (25 % of the fare on average), cash trips show 0 %. Cash tips are simply not recorded, so "tip rate" is only meaningful for card payments.

Gold tables for the full run are in `results/`.

## Run it

```bash
pip install duckdb pandas pyarrow pytest      # + pyspark and Java 17 for the Spark engine
pytest -q                                      # the Spark parity test is skipped without pyspark
# data: yellow_tripdata_2024-0{1,2,3}.parquet + taxi_zone_lookup.csv from the NYC TLC site, in data/raw/
python scripts/run_pipeline.py
```

## Licence

MIT
