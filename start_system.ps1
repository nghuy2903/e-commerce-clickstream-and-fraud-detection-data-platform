# # start_system.ps1
# Clear-Host
# Write-Host "🚀 KHỞI ĐỘNG HỆ THỐNG BANKING FRAUD DETECTION 🚀" -ForegroundColor Yellow
# Write-Host "=================================================" -ForegroundColor Yellow

# # 1. Khởi động hệ sinh thái Docker ngầm
# Write-Host "`n[1/5] Đang khởi động hạ tầng Docker Compose..." -ForegroundColor Cyan
# docker-compose up -d
# Write-Host "⏳ Đợi 15 giây để Kafka và Spark khởi động hoàn toàn..." -ForegroundColor DarkGray
# Start-Sleep -Seconds 15

# # 2. Dọn dẹp Checkpoint và Khởi tạo Lakehouse Iceberg
# Write-Host "`n[2/5] Đang dọn dẹp Checkpoint và tạo bảng Iceberg..." -ForegroundColor Cyan
# docker exec spark-master bash -c "rm -rf /tmp/checkpoints/*"
# docker exec spark-master /spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 /app/batch_layer/jobs/init_iceberg_tables.py

# 3. Kích hoạt Flink Real-time (Chạy ngầm với cờ -d)
Write-Host "`n[3/5] Đang bắn Job Flink Real-time xuống JobManager..." -ForegroundColor Cyan
docker exec -d -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e POSTGRES_HOST=postgres flink-jobmanager ./bin/flink run -py /tmp/fraud_detector.py

# 4. Kích hoạt Spark Streaming (Chạy ngầm với cờ -d)
Write-Host "`n[4/5] Đang bắn Job Spark Streaming xuống Spark Master..." -ForegroundColor Cyan
docker exec -d spark-master /spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 /app/batch_layer/jobs/ingest_kafka_to_iceberg.py

# 5. Bật FastAPI Backend (Mở trong cửa sổ PowerShell mới để bạn dễ theo dõi log)
Write-Host "`n[5/5] Đang khởi động Backend API..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "uvicorn serving_layer.api:app --host 0.0.0.0 --port 8000 --reload"

Write-Host "`n✅ HỆ THỐNG ĐÃ SẴN SÀNG!" -ForegroundColor Green
Write-Host "👉 Hãy click đúp vào file serving_layer/index.html để trải nghiệm." -ForegroundColor White

#6. Chạy sql trong warehouse
# docker exec spark-master /spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 /app/batch_layer/jobs/export_warehouse.py