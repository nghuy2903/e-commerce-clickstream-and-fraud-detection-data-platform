"""
Job: Batch Feature Pipeline cho mô hình XGBoost Fraud Detection.

Luồng xử lý:
  1. Đọc sự kiện thô từ Iceberg (banking_db.raw_banking_events)
  2. Gán nhãn is_fraud từ ground truth bots + IP rủi ro cao
  3. Trích xuất đặc trưng rolling 1 giờ bằng Window Functions
  4. Loại bỏ metadata và ID để tránh data leakage
  5. Ghi đè bảng đích banking_db.ml_fraud_features

Chạy local:
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \\
    batch_layer/jobs/batch_feature_pipeline.py

Chạy Docker:
  docker exec spark-master spark-submit \\
    --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \\
    /app/batch_layer/jobs/batch_feature_pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark

# --- Cấu hình bảng & nguồn ---
RAW_TABLE = "local.raw.raw_banking_events"
OUTPUT_TABLE = "local.raw.ml_fraud_features"
GROUND_TRUTH_FILE = Path(__file__).resolve().parent / "ground_truth_bots.csv"

# IP được biết là rủi ro cao (đồng bộ với data generator)
HIGH_RISK_IPS = ("192.168.1.99", "10.0.0.88", "203.0.113.42")

# Cột metadata cần loại bỏ trước khi huấn luyện ML
METADATA_COLUMNS = (
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
    "event_date",
    "is_simulated",
)

# Cột định danh — phải drop sau feature engineering để tránh leakage
IDENTIFIER_COLUMNS = ("user_id", "event_id")

# Rolling window 1 giờ (tính bằng giây khi dùng rangeBetween trên unix timestamp)
ONE_HOUR_SECONDS = 3600


def read_raw_events(spark: SparkSession) -> DataFrame:
    """Đọc toàn bộ sự kiện ngân hàng thô từ bảng Iceberg."""
    print(f"Đang đọc dữ liệu thô từ {RAW_TABLE}...")
    return spark.read.table(RAW_TABLE)


def read_ground_truth_bots(spark: SparkSession) -> DataFrame:
    """
    Đọc danh sách user_id bot từ CSV ground truth.
    File nằm cùng thư mục với job (batch_layer/jobs/ground_truth_bots.csv).
    """
    print(f"Đang đọc ground truth bots từ {GROUND_TRUTH_FILE}...")
    return (
        spark.read.option("header", True)
        .csv(str(GROUND_TRUTH_FILE))
        .select(F.col("user_id").alias("bot_user_id"))
        .distinct()
    )


def apply_fraud_labels(raw_df: DataFrame, bot_users_df: DataFrame) -> DataFrame:
    """
    Gán nhãn is_fraud (0/1):
      - 1 nếu user_id thuộc danh sách bot HOẶC ip_address thuộc HIGH_RISK_IPS
      - 0 cho các trường hợp còn lại
    """
    print("Đang gán nhãn is_fraud...")

    # Broadcast join nhỏ (danh sách bot) để tối ưu hiệu năng
    labeled_df = (
        raw_df.join(
            F.broadcast(bot_users_df),
            raw_df.user_id == bot_users_df.bot_user_id,
            how="left",
        )
        .withColumn(
            "is_fraud",
            F.when(
                F.col("bot_user_id").isNotNull()
                | F.col("ip_address").isin(*HIGH_RISK_IPS),
                F.lit(1),
            )
            .otherwise(F.lit(0))
            .cast("int"),
        )
        .drop("bot_user_id")
    )

    return labeled_df


def build_rolling_window_features(labeled_df: DataFrame) -> DataFrame:
    """
    Tạo đặc trưng rolling 1 giờ theo từng user, sắp xếp theo event_timestamp.

    - tx_count_1h:    số giao dịch trong 1 giờ qua (bao gồm dòng hiện tại)
    - amount_avg_1h:  trung bình amount trong 1 giờ qua
    - amount_vs_avg:  tỷ lệ amount so với trung bình (xử lý chia cho 0)
    """
    print("Đang trích xuất đặc trưng bằng Window Functions (rolling 1h)...")

    # rangeBetween yêu cầu cột orderBy kiểu số → dùng unix timestamp
    user_time_window = (
        Window.partitionBy("user_id")
        .orderBy(F.unix_timestamp("event_timestamp"))
        .rangeBetween(-ONE_HOUR_SECONDS, Window.currentRow)
    )

    featured_df = (
        labeled_df.withColumn("tx_count_1h", F.count("*").over(user_time_window))
        .withColumn("amount_avg_1h", F.avg("amount").over(user_time_window))
        .withColumn(
            "amount_vs_avg",
            F.when(
                F.col("amount_avg_1h").isNull() | (F.col("amount_avg_1h") == 0),
                F.lit(0.0),
            ).otherwise(F.col("amount") / F.col("amount_avg_1h")),
        )
    )

    return featured_df


def cleanse_and_prepare_ml_dataset(featured_df: DataFrame) -> DataFrame:
    """
    Làm sạch dữ liệu cho ML:
      - Drop metadata không cần thiết
      - Drop user_id, event_id để tránh data leakage
      - Chuyển event_timestamp → hour_of_day (đặc trưng thời gian)
    """
    print("Đang làm sạch dữ liệu và loại bỏ cột gây leakage...")

    ml_df = (
        featured_df.withColumn("hour_of_day", F.hour("event_timestamp"))
        .drop(*METADATA_COLUMNS, *IDENTIFIER_COLUMNS, "event_timestamp")
    )

    return ml_df


def write_ml_features(spark: SparkSession, ml_df: DataFrame) -> None:
    """Ghi đè (overwrite) DataFrame đặc trưng vào bảng Iceberg đích."""
    print(f"Đang ghi đè bảng đích {OUTPUT_TABLE}...")
    
    # 1. Lưu vào Iceberg (Giữ chuẩn Data Warehouse)
    (
        ml_df.write.format("iceberg")
        .mode("overwrite")
        .option("overwrite-mode", "replace")
        .saveAsTable(OUTPUT_TABLE)
    )
    
    # 2. Xuất ra file CSV duy nhất để mang lên Colab
    csv_output_dir = str(_ROOT / "batch_layer" / "ml_ready_data_csv")
    print(f"Đang xuất file CSV phục vụ Colab ra thư mục: {csv_output_dir}...")
    (
        ml_df.coalesce(1)  # Bắt buộc: Ép Spark gộp tất cả thành 1 file CSV duy nhất
        .write.mode("overwrite")
        .option("header", "true")
        .csv(csv_output_dir)
    )

    row_count = spark.read.table(OUTPUT_TABLE).count()
    print(f"Hoàn tất — Đã lưu {row_count:,} dòng dữ liệu sẵn sàng cho ML.")


def run_feature_pipeline(spark: SparkSession) -> DataFrame:
    """Orchestrate toàn bộ pipeline: đọc → label → feature → cleanse → ghi."""
    raw_df = read_raw_events(spark)
    bot_users_df = read_ground_truth_bots(spark)

    labeled_df = apply_fraud_labels(raw_df, bot_users_df)
    featured_df = build_rolling_window_features(labeled_df)
    ml_df = cleanse_and_prepare_ml_dataset(featured_df)

    write_ml_features(spark, ml_df)
    return ml_df


def main() -> None:
    spark = build_iceberg_spark(app_name="batch_feature_pipeline")
    spark.sparkContext.setLogLevel("WARN")

    try:
        ml_df = run_feature_pipeline(spark)
        print("Schema bảng đặc trưng ML:")
        ml_df.printSchema()
        print("Mẫu dữ liệu (5 dòng đầu):")
        ml_df.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
