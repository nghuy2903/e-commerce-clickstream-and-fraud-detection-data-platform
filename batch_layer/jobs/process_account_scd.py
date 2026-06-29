"""
Tầng 3 — Luồng Micro-batch (PySpark): SCD Type 2 account_history + MLOps evaluate.

Nguồn:  local.raw.raw_banking_events (Iceberg)
Đích:   local.dim.account_history (SCD2)

Chạy local:
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \\
    batch_layer/jobs/process_account_scd.py

Chạy Docker:
  docker exec spark-master spark-submit \\
    --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \\
    /app/batch_layer/jobs/process_account_scd.py
"""

from __future__ import annotations

import os
import random
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark
from batch_layer.modules.account_history_scd import merge_account_history_scd

RAW_TABLE = "local.raw.raw_banking_events"
ACCOUNT_HISTORY_TABLE = "local.dim.account_history"
EVALUATION_TABLE = "local.dim.model_evaluation_staging"

DEFAULT_OPENING_BALANCE = 10_000.00
BALANCE_EVENT_TYPES = ("WITHDRAW", "TRANSFER", "DEPOSIT")


def _champion_predict(amount: float, event_type: str, ip_address: str) -> bool:
    """Placeholder Champion Model — thay bằng model thật trong production."""
    if amount > 100_000:
        return True
    if event_type == "TRANSFER" and amount > 50_000:
        return random.random() < 0.7
    return random.random() < 0.05


def _challenger_predict(amount: float, event_type: str, ip_address: str) -> bool:
    """Placeholder Challenger Model — phiên bản thử nghiệm để A/B so sánh."""
    suspicious_ips = ("192.168.1.99", "10.0.0.88", "203.0.113.42")
    if ip_address in suspicious_ips:
        return True
    if event_type in ("WITHDRAW", "TRANSFER") and amount > 75_000:
        return random.random() < 0.6
    return random.random() < 0.08


def evaluate_models(df: DataFrame) -> DataFrame:
    """
    Khung MLOps: chạy song song Champion vs Challenger trên cùng dataframe.

    Thêm 2 cột `champion_is_fraud` và `challenger_is_fraud` để đối chiếu offline.
    Production: thay UDF bằng pandas UDF / MLflow model hoặc Spark ML Pipeline.
    """
    champion_udf = F.udf(_champion_predict, BooleanType())
    challenger_udf = F.udf(_challenger_predict, BooleanType())

    return (
        df.withColumn(
            "champion_is_fraud",
            champion_udf(F.col("amount"), F.col("event_type"), F.col("ip_address")),
        ).withColumn(
            "challenger_is_fraud",
            challenger_udf(F.col("amount"), F.col("event_type"), F.col("ip_address")),
        )
    )


def build_account_balance_snapshot(raw_df: DataFrame) -> DataFrame:
    """
    Tính snapshot balance hiện tại per user bằng Window Functions.

    Thuật toán SCD Type 2 — bước chuẩn bị snapshot:

    1. Lọc event ảnh hưởng số dư (WITHDRAW/TRANSFER trừ tiền, DEPOSIT cộng tiền).
    2. Sắp xếp theo (user_id, event_timestamp, event_id) để thứ tự deterministic.
    3. Tính delta từng event:
         WITHDRAW, TRANSFER → -amount
         DEPOSIT            → +amount
    4. Cumulative sum (ROWS UNBOUNDED PRECEDING) + opening balance → balance sau mỗi event.
    5. Lấy dòng cuối cùng mỗi user (ROW_NUMBER DESC) = snapshot hiện tại.
    6. User chưa có giao dịch balance → gán DEFAULT_OPENING_BALANCE.

    Output: account_id, user_id, balance, account_status, source_event_id
    """
    all_users = raw_df.select("user_id").distinct()

    balance_events = raw_df.filter(F.col("event_type").isin(*BALANCE_EVENT_TYPES))

    delta_col = (
        F.when(F.col("event_type").isin("WITHDRAW", "TRANSFER"), -F.col("amount"))
        .when(F.col("event_type") == "DEPOSIT", F.col("amount"))
        .otherwise(F.lit(0))
    )

    user_event_order = Window.partitionBy("user_id").orderBy(
        F.col("event_timestamp").asc(),
        F.col("event_id").asc(),
    )

    running_balance_df = (
        balance_events.withColumn("balance_delta", delta_col)
        .withColumn(
            "balance_after_event",
            F.lit(DEFAULT_OPENING_BALANCE)
            + F.sum("balance_delta").over(
                user_event_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    latest_event_window = Window.partitionBy("user_id").orderBy(
        F.col("event_timestamp").desc(),
        F.col("event_id").desc(),
    )

    latest_per_user = (
        running_balance_df.withColumn("row_rank", F.row_number().over(latest_event_window))
        .filter(F.col("row_rank") == 1)
        .select(
            F.col("user_id"),
            F.col("balance_after_event").alias("balance"),
            F.col("event_id").alias("source_event_id"),
        )
    )

    # Đọc danh sách user bị cảnh báo HIGH/CRITICAL từ PostgreSQL
    spark = SparkSession.getActiveSession()
    alerts_df = spark.read.jdbc(
        url="jdbc:postgresql://postgres:5432/banking_mlops",
        table="fraud_alerts",
        properties={
            "user": "admin",
            "password": "admin123",
            "driver": "org.postgresql.Driver"
        }
    ).filter("risk_level IN ('HIGH', 'CRITICAL')").select("user_id").distinct().withColumn("is_flagged", F.lit(True))

    snapshot = (
        all_users.join(latest_per_user, on="user_id", how="left")
        .join(alerts_df, on="user_id", how="left")
        .withColumn(
            "balance",
            F.coalesce(F.col("balance"), F.lit(DEFAULT_OPENING_BALANCE)).cast("decimal(18,2)"),
        )
        .withColumn(
            "account_id",
            F.col("user_id"),
        )
        .withColumn(
            "account_status",
            F.when(F.col("is_flagged") == True, F.lit("LOCKED")).otherwise(F.lit("ACTIVE")),
        )
        .withColumn(
            "status_reason",
            F.when(F.col("is_flagged") == True, F.lit("Suspicious activity flagged by ML model")).otherwise(F.lit(None).cast("string")),
        )
        .select(
            "account_id",
            "user_id",
            "balance",
            "account_status",
            "status_reason",
            "source_event_id",
        )
    )

    return snapshot


def debug_balance_audit(spark: SparkSession, raw_df: DataFrame, user_id: str | None = None) -> None:
    """
    In audit trail balance để debug khi số dư sai.

    Gọi hàm này với user_id cụ thể để xem từng bước cumulative sum.
    """
    balance_events = raw_df.filter(F.col("event_type").isin(*BALANCE_EVENT_TYPES))
    if user_id:
        balance_events = balance_events.filter(F.col("user_id") == user_id)

    delta_col = (
        F.when(F.col("event_type").isin("WITHDRAW", "TRANSFER"), -F.col("amount"))
        .when(F.col("event_type") == "DEPOSIT", F.col("amount"))
        .otherwise(F.lit(0))
    )

    w = Window.partitionBy("user_id").orderBy(
        F.col("event_timestamp").asc(),
        F.col("event_id").asc(),
    )

    audit_df = (
        balance_events.withColumn("balance_delta", delta_col)
        .withColumn(
            "running_balance",
            F.lit(DEFAULT_OPENING_BALANCE)
            + F.sum("balance_delta").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .select(
            "user_id",
            "event_id",
            "event_type",
            "amount",
            "balance_delta",
            "running_balance",
            "event_timestamp",
        )
        .orderBy("user_id", "event_timestamp")
    )

    print("=== DEBUG: Balance audit trail ===")
    audit_df.show(50, truncate=False)


def apply_scd_type_2_merge(
    spark: SparkSession,
    snapshot_df: DataFrame,
    batch_id: str,
) -> None:
    """
    SCD Type 2 trên Iceberg — hai pha MERGE + INSERT (Iceberg v2 safe pattern).

    Pha 1 — MERGE (đóng bản ghi cũ):
      JOIN target.is_current = true với snapshot mới theo account_id.
      Khi balance HOẶC account_status thay đổi:
        valid_to   = snapshot.valid_from (thời điểm đóng)
        is_current = false

    Pha 2 — INSERT (mở bản ghi mới):
      Chèn row mới khi:
        - account_id chưa tồn tại trong dimension, HOẶC
        - account_id có thay đổi (đã đóng ở pha 1, không còn current khớp snapshot).

    Chi tiết SQL nằm trong batch_layer/modules/account_history_scd.py.
    """
    merge_account_history_scd(
        spark=spark,
        snapshot_df=snapshot_df,
        batch_id=batch_id,
        target_table=ACCOUNT_HISTORY_TABLE,
    )


def persist_model_evaluation(spark: SparkSession, evaluated_df: DataFrame, batch_id: str) -> None:
    """Lưu kết quả so sánh model (staging) — tùy chọn cho MLOps dashboard."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EVALUATION_TABLE} (
            event_id            STRING,
            user_id             STRING,
            event_type          STRING,
            amount              DECIMAL(18, 2),
            champion_is_fraud   BOOLEAN,
            challenger_is_fraud BOOLEAN,
            batch_id            STRING,
            evaluated_at        TIMESTAMP
        )
        USING iceberg
        """
    )

    (
        evaluated_df.select(
            "event_id",
            "user_id",
            "event_type",
            "amount",
            "champion_is_fraud",
            "challenger_is_fraud",
            F.lit(batch_id).alias("batch_id"),
            F.current_timestamp().alias("evaluated_at"),
        ).writeTo(EVALUATION_TABLE)
        .append()
    )


def main() -> None:
    batch_id = f"scd_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    debug_user = os.environ.get("DEBUG_USER_ID")

    spark = build_iceberg_spark(app_name="process_account_scd")
    try:
        print(f"[START] Micro-batch SCD job | batch_id={batch_id}")

        raw_df = spark.table(RAW_TABLE)
        row_count = raw_df.count()
        print(f"[SOURCE] Đọc {row_count} dòng từ {RAW_TABLE}")

        if row_count == 0:
            print("[SKIP] Không có dữ liệu thô — kết thúc job.")
            return

        if debug_user:
            debug_balance_audit(spark, raw_df, user_id=debug_user)
        elif os.environ.get("DEBUG_BALANCE_AUDIT", "").lower() in ("1", "true", "yes"):
            debug_balance_audit(spark, raw_df)

        snapshot_df = build_account_balance_snapshot(raw_df)
        print("[SCD] Snapshot balance hiện tại:")
        snapshot_df.orderBy("user_id").show(20, truncate=False)

        apply_scd_type_2_merge(spark, snapshot_df, batch_id=batch_id)
        print(f"[SCD] Đã merge vào {ACCOUNT_HISTORY_TABLE}")

        evaluated_df = evaluate_models(raw_df)
        disagreement = evaluated_df.filter(
            F.col("champion_is_fraud") != F.col("challenger_is_fraud")
        ).count()
        total = evaluated_df.count()
        print(f"[MLOPS] Champion vs Challenger disagreement: {disagreement}/{total}")

        persist_model_evaluation(spark, evaluated_df, batch_id=batch_id)
        print(f"[MLOPS] Đã ghi staging → {EVALUATION_TABLE}")

        current_rows = (
            spark.table(ACCOUNT_HISTORY_TABLE)
            .filter(F.col("is_current") == True)
            .orderBy("user_id")
        )
        print("[VERIFY] Dòng is_current=true sau merge:")
        current_rows.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
