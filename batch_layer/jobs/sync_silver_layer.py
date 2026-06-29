"""
PySpark job: Sync Bronze raw events to Silver cleaned transactions in Apache Iceberg.
"""
from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_TABLE = "local.raw.raw_banking_events"
SILVER_TABLE = "local.silver.transactions"

def create_silver_table_if_not_exists(spark: SparkSession):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            transaction_id      STRING,
            account_id          STRING,
            user_id             STRING,
            event_type          STRING,
            amount              DECIMAL(18, 2),
            currency            STRING,
            ip_address          STRING,
            is_simulated        BOOLEAN,
            event_timestamp     TIMESTAMP,
            ingested_at         TIMESTAMP,
            event_date          DATE
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)
    print(f"✅ Bảng Silver Iceberg '{SILVER_TABLE}' đã sẵn sàng.")

def run_silver_pipeline(spark: SparkSession):
    create_silver_table_if_not_exists(spark)

    print("📥 Đang đọc dữ liệu thô từ Bronze...")
    raw_df = spark.read.table(RAW_TABLE)

    # 1. Loại bỏ các giao dịch trùng lặp theo event_id
    cleaned_df = raw_df.dropDuplicates(["event_id"])

    # 2. Lọc các giao dịch có event_type hợp lệ
    valid_tx_df = cleaned_df.filter("event_type IN ('TRANSFER', 'WITHDRAW', 'DEPOSIT')")

    # 3. Trực tiếp ánh xạ account_id bằng user_id (mỗi user coi như một tài khoản)
    silver_tx_df = valid_tx_df.select(
        F.col("event_id").alias("transaction_id"),
        F.col("user_id").alias("account_id"),
        F.col("user_id"),
        F.col("event_type"),
        F.col("amount"),
        F.lit("VND").alias("currency"),
        F.col("ip_address"),
        F.col("is_simulated"),
        F.col("event_timestamp"),
        F.col("ingested_at"),
        F.col("event_date")
    )

    print(f"💾 Đang ghi dữ liệu vào tầng Silver Iceberg '{SILVER_TABLE}'...")
    (
        silver_tx_df.write.format("iceberg")
        .mode("overwrite")
        .option("overwrite-mode", "replace") # Ghi đè thay thế phân vùng
        .saveAsTable(SILVER_TABLE)
    )

    total_rows = spark.read.table(SILVER_TABLE).count()
    print(f"🎉 Hoàn tất đồng bộ tầng Silver. Tổng số dòng: {total_rows}")

def main():
    spark = build_iceberg_spark(app_name="sync_silver_layer")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_silver_pipeline(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
