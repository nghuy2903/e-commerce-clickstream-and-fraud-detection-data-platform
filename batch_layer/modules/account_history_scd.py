"""
SCD Type 2 merge cho bảng Iceberg `local.dim.account_history`.

Dùng sau micro-batch Spark: so sánh snapshot mới với dòng `is_current = true`,
đóng bản cũ (`valid_to`, `is_current = false`) và chèn phiên bản mới.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ACCOUNT_HISTORY_TABLE = "local.dim.account_history"
REQUIRED_SNAPSHOT_COLUMNS = (
    "account_id",
    "user_id",
    "balance",
    "account_status",
)


def validate_snapshot(snapshot_df: DataFrame) -> None:
    missing = [c for c in REQUIRED_SNAPSHOT_COLUMNS if c not in snapshot_df.columns]
    if missing:
        raise ValueError(f"Snapshot thiếu cột bắt buộc: {', '.join(missing)}")


def prepare_incoming_versions(
    snapshot_df: DataFrame,
    batch_id: str,
    effective_time: F.Column | None = None,
) -> DataFrame:
    """Chuẩn hóa snapshot micro-batch thành các phiên bản SCD sắp merge."""
    validate_snapshot(snapshot_df)
    ts = effective_time if effective_time is not None else F.current_timestamp()

    status_reason_col = (
        F.col("status_reason")
        if "status_reason" in snapshot_df.columns
        else F.lit(None).cast("string").alias("status_reason")
    )
    source_event_col = (
        F.col("source_event_id")
        if "source_event_id" in snapshot_df.columns
        else F.lit(None).cast("string").alias("source_event_id")
    )

    return (
        snapshot_df.select(
            "account_id",
            "user_id",
            F.col("balance").cast("decimal(18,2)"),
            F.col("account_status"),
            status_reason_col,
            source_event_col,
        )
        .withColumn("valid_from", ts)
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("created_at", ts)
    )


def merge_account_history_scd(
    spark: SparkSession,
    snapshot_df: DataFrame,
    batch_id: str,
    target_table: str = ACCOUNT_HISTORY_TABLE,
) -> None:
    """
    Hai bước MERGE + INSERT (pattern an toàn trên Iceberg v2):

    1. MERGE: đóng dòng current khi balance hoặc account_status thay đổi.
    2. INSERT: thêm phiên bản mới (account mới hoặc vừa đóng ở bước 1).
    """
    incoming_df = prepare_incoming_versions(snapshot_df, batch_id=batch_id)
    incoming_df.createOrReplaceTempView("incoming_account_snapshot")

    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING incoming_account_snapshot AS source
        ON target.account_id = source.account_id
           AND target.is_current = true
        WHEN MATCHED AND (
            target.balance <> source.balance
            OR target.account_status <> source.account_status
        ) THEN UPDATE SET
            valid_to   = source.valid_from,
            is_current = false
        """
    )

    spark.sql(
        f"""
        INSERT INTO {target_table} (
            account_id,
            user_id,
            balance,
            account_status,
            status_reason,
            valid_from,
            valid_to,
            is_current,
            source_event_id,
            batch_id,
            created_at
        )
        SELECT
            s.account_id,
            s.user_id,
            s.balance,
            s.account_status,
            s.status_reason,
            s.valid_from,
            s.valid_to,
            s.is_current,
            s.source_event_id,
            s.batch_id,
            s.created_at
        FROM incoming_account_snapshot s
        LEFT JOIN {target_table} t
            ON t.account_id = s.account_id
           AND t.is_current = true
           AND t.balance = s.balance
           AND t.account_status = s.account_status
        WHERE t.account_id IS NULL
        """
    )
