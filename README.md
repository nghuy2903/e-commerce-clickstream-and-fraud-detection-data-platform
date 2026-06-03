# Kiến trúc Hệ thống: E-commerce Clickstream & Fraud Detection Data Platform

Dự án này xây dựng một nền tảng dữ liệu (Data Platform) thu nhỏ, mô phỏng luồng xử lý sự kiện clickstream của người dùng trên nền tảng thương mại điện tử và phát hiện lưu lượng truy cập bất thường (Bot) theo thời gian thực và xử lý lô (Batch).

## 1. Sơ đồ Kiến trúc Tổng thể (Architecture Diagram)

![Sơ đồ kiến trúc Data Engineering](images/architecture_diagram.png)

Kết nối từ máy host
Kafka: localhost:9092 (topic banking_events)
Kafka (trong Docker network): kafka:29092
Flink UI: http://localhost:8081
Spark UI: http://localhost:8080
PostgreSQL: postgresql://admin:admin123@localhost:5432/banking_mlops