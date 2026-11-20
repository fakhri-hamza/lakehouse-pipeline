"""The same pipeline on Spark (PySpark), using the SQL text shared with DuckDB.

Run in CI on a fixture, where the gold tables of both engines are compared row by row.
"""
from __future__ import annotations

from pathlib import Path

from lakehouse import sql
from lakehouse.duck import month_bounds, month_of


def session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]").appName("lakehouse")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def run(spark, files: list[Path], zones_csv: Path, out: Path) -> dict:
    """bronze -> silver -> gold for the given monthly files; returns reject counts per month."""
    spark.read.csv(str(zones_csv), header=True, inferSchema=True).createOrReplaceTempView("zones")
    silver_parts, report = [], {}
    for f in sorted(files):
        month = month_of(f)
        start, end = month_bounds(month)
        spark.read.parquet(str(f)).createOrReplaceTempView("bronze")
        tagged = spark.sql(sql.tagged("spark", start, end))
        tagged.cache()
        report[month] = {r["reason"] or "ok": r["n"] for r in
                         tagged.groupBy(tagged.reject_reason.alias("reason")).count()
                         .withColumnRenamed("count", "n").collect()}
        part = out / "silver" / f"month={month}"
        tagged.where("reject_reason IS NULL").selectExpr(*[c.strip() for c in sql.SILVER_COLS.split(",")]) \
            .write.mode("overwrite").parquet(str(part))          # overwrite = idempotent per month
        silver_parts.append(str(part))
        tagged.unpersist()
    spark.read.parquet(*silver_parts).createOrReplaceTempView("silver")
    for name, q in sql.gold("spark").items():
        spark.sql(q).write.mode("overwrite").parquet(str(out / "gold" / name))
    return report


def gold(spark, out: Path, name: str):
    return spark.read.parquet(str(out / "gold" / name)).orderBy(*sql.GOLD_ORDER[name]).toPandas()
