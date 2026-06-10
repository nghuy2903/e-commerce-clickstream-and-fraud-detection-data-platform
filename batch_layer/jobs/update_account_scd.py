"""
Job: Update Account SCD Type 2
Vai trò: Quét dữ liệu giao dịch mới nhất từ bảng raw, mô phỏng chấm điểm Fraud (Champion Model), 
tính toán lại số dư và cập nhật vào bảng lịch sử tài khoản theo cơ chế SCD Type 2.
"""

import sys
import uuid
import random
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Đảm bảo import được các module trong project
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark
from batch_layer.modules.account_history_scd import merge_account_history_scd

RAW_TABLE = "local.raw.raw_banking_events"
ACCOUNT_HISTORY_TABLE = "local.dim.account_history"

def simulate_champion_model_scoring(spark: SparkSession, df) -> DataFrame:
    """
    Giả lập mô hình Champion chấm điểm giao dịch.
    (Giống hệt logic đang chạy bên Flink Real-time)
    """
    print(" Đang chạy mô phỏng điểm số Fraud (Champion Model)...")
    
    # Định nghĩa UDF (User Defined Function) để sinh điểm ngẫu nhiên
    @F.udf(returnType="float")
    def random_risk_score():
        return round(random.uniform(0, 100), 2)
    
    # Threshold = 80 (như bên luồng Stream)
    scored_df = df.withColumn("risk_score", random_risk_score()) \
                  .withColumn("is_fraud", F.col("risk_score") > 80.0)
    return scored_df


def extract_latest_account_status(spark: SparkSession) -> DataFrame:
    """
    Đọc dữ liệu thô và tổng hợp trạng thái/số dư mới nhất của từng tài khoản.
    Trong thực tế, bạn sẽ cộng/trừ `amount` dựa trên `event_type` (DEPOSIT/WITHDRAWAL).
    Ở bản demo này, chúng ta lấy giao dịch mới nhất của user làm số dư hiện tại.
    """
    print(f" Đang đọc dữ liệu giao dịch mới nhất từ {RAW_TABLE}...")
    
    raw_df = spark.read.table(RAW_TABLE)
    
    # Nếu bảng thô đang trống
    if raw_df.count() == 0:
        return None

    # Bước 1: Mô phỏng chạy Model Champion
    scored_df = simulate_champion_model_scoring(spark, raw_df)

    # Bước 2: Xác định trạng thái tài khoản dựa trên kết quả Fraud
    # Nếu bất kỳ giao dịch nào của user bị đánh dấu is_fraud = True, tài khoản đó bị LOCKED
    status_df = scored_df.withColumn(
        "account_status",
        F.when(F.col("is_fraud") == True, F.lit("LOCKED")).otherwise(F.lit("ACTIVE"))
    )

    # Bước 3: Lấy giao dịch cuối cùng của mỗi user để làm Snapshot mới nhất
    # Dùng Window Function để tìm dòng mới nhất
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("user_id").orderBy(F.col("event_timestamp").desc())
    
    latest_snapshot_df = status_df.withColumn("row_num", F.row_number().over(window_spec)) \
                                  .filter(F.col("row_num") == 1) \
                                  .drop("row_num")

    # Chuẩn bị Schema khớp với hàm SCD của bạn
    # Giả định: account_id chính là user_id trong hệ thống demo này
    prepared_snapshot = latest_snapshot_df.select(
        F.col("user_id").alias("account_id"),
        "user_id",
        F.col("amount").alias("balance"), # Lấy tạm amount làm balance để demo
        "account_status",
        F.lit("Xử lý qua Micro-batch").alias("status_reason"),
        "event_id", # Dùng làm source_event_id
    ).withColumnRenamed("event_id", "source_event_id")

    return prepared_snapshot


def main():
    spark = build_iceberg_spark(app_name="update_scd_type_2")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # # 1. Trích xuất trạng thái tài khoản mới nhất
        # snapshot_df = extract_latest_account_status(spark)
        
        # if snapshot_df is None:
        #     print("Trống trải quá! Không có giao dịch nào mới để cập nhật SCD.")
        #     return

        # batch_id = f"batch_{uuid.uuid4().hex[:8]}"
        # print(f"Bắt đầu chạy MERGE SCD Type 2 (Batch ID: {batch_id})...")

        # # 2. Gọi module SCD chuẩn của bạn để cập nhật vào Lakehouse
        # merge_account_history_scd(
        #     spark=spark,
        #     snapshot_df=snapshot_df,
        #     batch_id=batch_id,
        #     target_table=ACCOUNT_HISTORY_TABLE
        # )
        
        # print(f"Hoàn thành cập nhật SCD! Dữ liệu đã được ghi vào {ACCOUNT_HISTORY_TABLE}.")

        # In thử 5 dòng lịch sử ra màn hình để kiểm tra
        print("\n--- 5 Dòng Lịch Sử Tài Khoản Gần Nhất ---")
        spark.sql(f"""
            SELECT account_id, balance, account_status, is_current, valid_to 
            FROM {ACCOUNT_HISTORY_TABLE} 
            ORDER BY created_at DESC LIMIT 5
        """).show(truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()