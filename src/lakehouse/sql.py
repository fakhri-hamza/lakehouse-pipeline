"""The pipeline's SQL, written once and run by two engines (DuckDB locally, Spark in CI).

Only two things differ between the dialects: the trip duration in seconds and how a file is read.
Everything else (the quality rules, deduplication, the gold KPIs) is the same text, so the
two engines are checked against each other in the tests.
"""
from __future__ import annotations

DURATION_S = {
    "duckdb": "date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime)",
    "spark": "(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))",
}

# ISO day of week, 1 = Monday ... 7 = Sunday (DuckDB's dayofweek and Spark's start on different days)
DOW = {"duckdb": "isodow(tpep_pickup_datetime)", "spark": "(weekday(tpep_pickup_datetime) + 1)"}

# Quality rules: (name, condition a VALID row satisfies). Order matters: a quarantined row is
# tagged with the first rule it breaks.
RULES = [
    ("pickup_in_month",       "tpep_pickup_datetime >= TIMESTAMP '{start}' AND tpep_pickup_datetime < TIMESTAMP '{end}'"),
    ("dropoff_after_pickup",  "tpep_dropoff_datetime > tpep_pickup_datetime"),
    ("duration_le_6h",        "{duration} <= 21600"),
    ("distance_in_range",     "trip_distance > 0 AND trip_distance <= 200"),
    ("amounts_non_negative",  "fare_amount >= 0 AND total_amount >= 0 AND tip_amount >= 0"),
    ("known_zones",           "PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263"),
]

KEY = "VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, total_amount"


def tagged(engine: str, start: str, end: str, src: str = "bronze") -> str:
    """Every bronze row with `reject_reason` = first broken rule, 'duplicate', or NULL if clean."""
    dur = DURATION_S[engine]
    cases = "\n".join(f"      WHEN NOT coalesce({cond.format(start=start, end=end, duration=dur)}, false) THEN '{name}'"
                      for name, cond in RULES)
    return f"""
WITH numbered AS (
  SELECT *, {dur} AS duration_s,
         ROW_NUMBER() OVER (PARTITION BY {KEY} ORDER BY tpep_pickup_datetime) AS dup_rank
  FROM {src}
)
SELECT *,
  CASE
{cases}
      WHEN dup_rank > 1 THEN 'duplicate'
      ELSE NULL
  END AS reject_reason
FROM numbered"""


SILVER_COLS = """VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
  PULocationID, DOLocationID, payment_type, fare_amount, tip_amount, tolls_amount, total_amount, duration_s"""

GOLD = {
    # one row per day and pickup borough
    "daily_borough": """
SELECT CAST(t.tpep_pickup_datetime AS DATE) AS day, z.Borough AS borough,
       COUNT(*) AS trips,
       ROUND(SUM(t.total_amount), 2) AS revenue,
       ROUND(AVG(t.trip_distance), 3) AS avg_miles,
       ROUND(AVG(t.duration_s) / 60.0, 2) AS avg_minutes
FROM silver t JOIN zones z ON t.PULocationID = z.LocationID
GROUP BY CAST(t.tpep_pickup_datetime AS DATE), z.Borough""",
    # demand profile: average trips per hour of week
    "hourly_demand": """
SELECT {dow} AS dow, hour(tpep_pickup_datetime) AS hour, COUNT(*) AS trips
FROM silver
GROUP BY {dow}, hour(tpep_pickup_datetime)""",
    # tipping by payment type (cash tips are not recorded, which the README calls out)
    "tips_by_payment": """
SELECT payment_type, COUNT(*) AS trips,
       ROUND(AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount END), 4) AS avg_tip_rate,
       ROUND(AVG(CASE WHEN tip_amount > 0 THEN 1.0 ELSE 0.0 END), 4) AS share_tipped
FROM silver
GROUP BY payment_type""",
    # busiest zone-to-zone routes
    "top_routes": """
SELECT pz.Zone AS pickup_zone, dz.Zone AS dropoff_zone, COUNT(*) AS trips,
       ROUND(AVG(t.duration_s) / 60.0, 2) AS avg_minutes, ROUND(AVG(t.total_amount), 2) AS avg_total
FROM silver t
JOIN zones pz ON t.PULocationID = pz.LocationID
JOIN zones dz ON t.DOLocationID = dz.LocationID
GROUP BY pz.Zone, dz.Zone""",
}

def gold(engine: str) -> dict[str, str]:
    return {k: q.replace("{dow}", DOW[engine]) for k, q in GOLD.items()}


GOLD_ORDER = {
    "daily_borough": ["day", "borough"],
    "hourly_demand": ["dow", "hour"],
    "tips_by_payment": ["payment_type"],
    "top_routes": ["pickup_zone", "dropoff_zone"],
}
