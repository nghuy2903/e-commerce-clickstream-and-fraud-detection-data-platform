"""Cấu hình Spark session dùng chung cho Apache Iceberg (Hadoop catalog)."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

# Spark 3.3 (bde2020 image) + Iceberg 1.4.x runtime
ICEBERG_RUNTIME_PACKAGE = (
    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3"
)
DEFAULT_CATALOG = "local"
DEFAULT_WAREHOUSE = Path(__file__).resolve().parents[1] / "warehouse"


def warehouse_path() -> str:
    return os.environ.get("ICEBERG_WAREHOUSE", str(DEFAULT_WAREHOUSE))


def build_iceberg_spark(
    app_name: str = "banking_iceberg",
    master: str | None = None,
) -> SparkSession:
    """
    Tạo SparkSession với Iceberg extensions.

    Chạy trong Docker:
      docker exec spark-master spark-submit \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \\
        /app/batch_layer/jobs/init_iceberg_tables.py
    """
    warehouse = warehouse_path()
    Path(warehouse).mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ICEBERG_RUNTIME_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{DEFAULT_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{DEFAULT_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{DEFAULT_CATALOG}.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", DEFAULT_CATALOG)
        .config("spark.sql.catalogImplementation", "in-memory")
    )

    if master:
        builder = builder.master(master)

    return builder.getOrCreate()
