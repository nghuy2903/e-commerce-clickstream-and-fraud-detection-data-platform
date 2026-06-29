import sys
import uuid
import random
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark

DB_URL = "jdbc:postgresql://postgres:5432/banking_mlops?stringtype=unspecified"
DB_PROPERTIES = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

def main():
    spark = build_iceberg_spark(app_name="backfill_postgres_alerts")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("📥 Đang đọc dữ liệu thô từ Bronze...")
        raw_df = spark.read.table("local.raw.raw_banking_events")
        if raw_df.count() == 0:
            print("⚠️ Bảng raw trống trải, không có gì để backfill.")
            return

        # 1. Trích xuất tất cả user_id duy nhất và chuẩn bị ghi vào bảng users & accounts
        print("👤 Đang chuẩn bị danh mục Users và Accounts...")
        
        # UDF sinh UUID ngẫu nhiên hoặc xác định dựa trên user_id để tránh lỗi type UUID trong Postgres
        @F.udf(returnType="string")
        def get_deterministic_uuid(val):
            if not val:
                return str(uuid.uuid4())
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, val))

        users_df = raw_df.select("user_id").distinct().withColumn(
            "display_name", F.concat(F.lit("User "), F.substring(F.col("user_id"), 1, 8))
        ).withColumn(
            "email", F.concat(F.substring(F.col("user_id"), 1, 8), F.lit("@bank.com"))
        ).withColumn(
            "phone", F.lit("0912345678")
        ).withColumn(
            "is_simulated", F.lit(True)
        )

        # Ghi vào users trong Postgres
        users_df.write.jdbc(
            url=DB_URL,
            table="users",
            mode="append",
            properties=DB_PROPERTIES
        )

        accounts_df = users_df.select(
            get_deterministic_uuid(F.col("user_id")).alias("account_id"),
            "user_id",
            F.substring(F.regexp_replace(F.col("user_id"), "-", ""), 1, 32).alias("account_number"),
            F.lit("VND").alias("currency"),
            F.lit(10000000.00).alias("balance"),
            F.lit("ACTIVE").alias("status"),
            F.lit("Initial balance").alias("status_reason")
        )

        # Ghi vào accounts trong Postgres
        accounts_df.write.jdbc(
            url=DB_URL,
            table="accounts",
            mode="append",
            properties=DB_PROPERTIES
        )
        print("✅ Đã ghi danh mục Users và Accounts vào PostgreSQL.")

        # 2. Đọc file ground_truth_bots.csv để lấy danh sách bots
        print("🤖 Đang đọc danh sách Bots từ ground_truth_bots.csv...")
        bots_df = spark.read.option("header", "true").csv("/app/batch_layer/jobs/ground_truth_bots.csv")
        bot_users = [row["user_id"] for row in bots_df.collect()]

        # Các IP đáng ngờ
        suspicious_ips = ["192.168.1.99", "10.0.0.88", "203.0.113.42"]

        # Lọc giao dịch tài chính
        tx_df = raw_df.filter("event_type IN ('TRANSFER', 'WITHDRAW')")

        # Đánh dấu điều kiện gian lận
        fraud_cond = (F.col("user_id").isin(bot_users)) | (F.col("ip_address").isin(suspicious_ips))

        fraud_tx = tx_df.filter(fraud_cond)
        print(f"🔥 Phát hiện {fraud_tx.count()} giao dịch có dấu hiệu gian lận cần sinh cảnh báo.")

        if fraud_tx.count() > 0:
            # UDF sinh UUID ngẫu nhiên cho alert_id
            @F.udf(returnType="string")
            def make_uuid():
                return str(uuid.uuid4())

            # UDF sinh XGBoost và Isolation Forest scores
            # xgboost_score: 0.82 - 0.99 (risk_score = xgboost_score)
            # challenger_score (iforest_score): 75.0 - 99.0
            @F.udf(returnType="double")
            def gen_risk_score():
                return round(random.uniform(0.8200, 0.9999), 4)

            @F.udf(returnType="double")
            def gen_challenger_score():
                return round(random.uniform(75.0, 99.9), 2)

            alerts_to_write = fraud_tx.select(
                "user_id",
                get_deterministic_uuid(F.col("user_id")).alias("account_id"),
                F.col("event_id").alias("source_event_id"),
                gen_risk_score().alias("risk_score"),
                gen_challenger_score().alias("challenger_score"),
                F.col("event_timestamp").alias("detected_at"),
                F.col("event_timestamp").alias("created_at")
            ).withColumn(
                "risk_level",
                F.when(F.col("risk_score") > 0.93, F.lit("CRITICAL")).otherwise(F.lit("HIGH"))
            ).withColumn(
                "rule_name",
                F.when(F.col("user_id").isin(bot_users), F.lit("XGBoost Bot Fraud Pattern")).otherwise(F.lit("High Risk IP Alert"))
            ).withColumn(
                "alert_message",
                F.concat(F.lit("Giao dịch bất thường phát hiện từ IP: "), F.col("user_id"))
            )

            # Ghi vào Postgres
            print("💾 Đang ghi đè/nạp danh sách cảnh báo vào fraud_alerts trong Postgres...")
            alerts_to_write.write.jdbc(
                url=DB_URL,
                table="fraud_alerts",
                mode="append",
                properties=DB_PROPERTIES
            )
            print(f"🎉 Hoàn tất sinh {alerts_to_write.count()} cảnh báo gian lận thành công.")
        else:
            print("ℹ️ Không tìm thấy giao dịch gian lận nào.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
