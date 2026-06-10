"""
Khởi tạo bảng Apache Iceberg (Data Lakehouse) cho Banking Fraud Detection.

Bảng:
  - local.raw.raw_banking_events   (append-only, partition theo ngày event)
  - local.dim.account_history      (SCD Type 2)

Chạy local (cần JAR Iceberg qua --packages):
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \
    batch_layer/jobs/init_iceberg_tables.py

Chạy trong Docker:
  docker exec spark-master spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \
    /app/batch_layer/jobs/init_iceberg_tables.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Cho phép import batch_layer khi chạy trực tiếp file
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import DEFAULT_CATALOG, build_iceberg_spark

CATALOG = DEFAULT_CATALOG
RAW_NAMESPACE = f"{CATALOG}.raw"
DIM_NAMESPACE = f"{CATALOG}.dim"
RAW_TABLE = f"{RAW_NAMESPACE}.raw_banking_events"
ACCOUNT_HISTORY_TABLE = f"{DIM_NAMESPACE}.account_history"


def create_namespaces(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {RAW_NAMESPACE}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DIM_NAMESPACE}")


def create_raw_banking_events(spark) -> None:
    """
    Lưu toàn bộ log thô từ Kafka — append-only.
    - Đã gỡ bỏ NOT NULL để tương thích với luồng Streaming JSON.
    - Sử dụng cột vật lý event_date thay cho hàm days() để tránh bug của Spark 3.3 Streaming.
    """
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {RAW_TABLE} (
            event_id            STRING          COMMENT 'UUIDv4 từ Layer 1',
            event_timestamp     TIMESTAMP       COMMENT 'ISO 8601 UTC parsed',
            user_id             STRING,
            event_type          STRING,
            amount              DECIMAL(18, 2),
            ip_address          STRING,
            is_simulated        BOOLEAN,
            kafka_topic         STRING,
            kafka_partition     INT,
            kafka_offset        BIGINT,
            ingested_at         TIMESTAMP       COMMENT 'Thời điểm Spark ghi vào lake',
            event_date          DATE            COMMENT 'Cột vật lý dùng để Partition'
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'format-version' = '2',
            'comment' = 'Bronze — append-only banking events từ Kafka'
        )
        """
    )


def create_account_history_scd(spark) -> None:
    """
    SCD Type 2 — lịch sử thay đổi balance / status tài khoản sau mỗi micro-batch.
    (Hàm days() vẫn hoạt động bình thường cho Batch Processing nên được giữ nguyên)
    Đã gỡ bỏ ràng buộc NOT NULL để tương thích với quá trình transform của Spark DataFrame.
    """
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {ACCOUNT_HISTORY_TABLE} (
            account_id          STRING,
            user_id             STRING,
            balance             DECIMAL(18, 2),
            account_status      STRING          COMMENT 'ACTIVE|WARNING|LOCKED|SUSPENDED',
            status_reason       STRING,
            valid_from          TIMESTAMP,
            valid_to            TIMESTAMP       COMMENT 'NULL = đang mở (current row)',
            is_current          BOOLEAN,
            source_event_id     STRING          COMMENT 'event_id kích hoạt phiên bản mới',
            batch_id            STRING          COMMENT 'Spark micro-batch / job run id',
            created_at          TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(valid_from))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'format-version' = '2',
            'comment' = 'Silver — SCD2 account balance/status timeline'
        )
        """
    )

def main() -> None:
    spark = build_iceberg_spark(app_name="init_iceberg_tables")
    try:
        create_namespaces(spark)
        create_raw_banking_events(spark)
        create_account_history_scd(spark)
        print(f"Created namespaces: {RAW_NAMESPACE}, {DIM_NAMESPACE}")
        print(f"Created/Replaced table: {RAW_TABLE}")
        print(f"Created/Replaced table: {ACCOUNT_HISTORY_TABLE}")
        spark.sql(f"DESCRIBE TABLE EXTENDED {RAW_TABLE}").show(truncate=False)
        spark.sql(f"DESCRIBE TABLE EXTENDED {ACCOUNT_HISTORY_TABLE}").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()