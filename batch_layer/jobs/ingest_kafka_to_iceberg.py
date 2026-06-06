"""
Job: Ingest Kafka to Iceberg
Vai trò: Hút dữ liệu liên tục từ topic 'banking_events', parse JSON và lưu vào Iceberg.
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, BooleanType, TimestampType
)

# Đảm bảo import được config Iceberg từ project
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "banking_events"
RAW_TABLE = "local.raw.raw_banking_events"
CHECKPOINT_LOCATION = "/tmp/checkpoints/raw_events"

def main():
    spark = build_iceberg_spark(app_name="kafka_to_iceberg_ingestion")
    spark.sparkContext.setLogLevel("WARN")

    print(f" Bắt đầu luồng Ingestion từ Kafka ({KAFKA_TOPIC}) vào Iceberg ({RAW_TABLE})...")

    # 1. Định nghĩa Schema khớp chính xác với JSON sinh ra từ Layer 1
    json_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("ip_address", StringType(), True),
        StructField("is_simulated", BooleanType(), True)
    ])

    # 2. Đọc luồng dữ liệu thô từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Transform: Parse JSON, ép kiểu và TẠO CỘT PHÂN VÙNG (event_date)
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str", "topic", "partition", "offset") \
        .select(
            F.from_json(F.col("json_str"), json_schema).alias("data"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset")
        ) \
        .select("data.*", "kafka_topic", "kafka_partition", "kafka_offset") \
        .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp"))) \
        .withColumn("ingested_at", F.current_timestamp()) \
        .withColumn("event_date", F.to_date(F.col("timestamp"))) \
        .select(
            "event_id",
            "event_timestamp",
            "user_id",
            "event_type",
            "amount",
            "ip_address",
            "is_simulated",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "ingested_at",
            "event_date" # BẮT BUỘC PHẢI THÊM CỘT NÀY ĐỂ ICEBERG NHẬN DIỆN PHÂN VÙNG
        )

    # 4. Sink: Ghi dữ liệu liên tục vào Iceberg với Trigger 10 giây
    query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .option("fanout-enabled", "true") \
        .trigger(processingTime="30 seconds") \
        .option("path", RAW_TABLE) \
        .start()

    print(" Streaming đang chạy. Bấm Ctrl+C để dừng.")
    query.awaitTermination()

if __name__ == "__main__":
    main()