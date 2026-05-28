# Kiến trúc Hệ thống: E-commerce Clickstream & Fraud Detection Data Platform

Dự án này xây dựng một nền tảng dữ liệu (Data Platform) thu nhỏ, mô phỏng luồng xử lý sự kiện clickstream của người dùng trên nền tảng thương mại điện tử và phát hiện lưu lượng truy cập bất thường (Bot) theo thời gian thực và xử lý lô (Batch).

## 1. Sơ đồ Kiến trúc Tổng thể (Architecture Diagram)

```mermaid
flowchart TB
    %% Định nghĩa các node
    subgraph Data Generation ["Data Generation Layer"]
        PyGen([Python Script<br/>Event Generator])
    end

    subgraph Ingestion ["Ingestion Layer"]
        Kafka{Apache Kafka<br/>KRaft Mode}
    end

    subgraph Processing ["Processing Layer"]
        direction LR
        Flink([Apache Flink<br/>Real-time Streaming])
        Spark([PySpark<br/>Micro-batch Processing])
    end

    subgraph Storage ["Storage Layer"]
        PG[(PostgreSQL<br/>Serving DB)]
        Iceberg[(Apache Iceberg<br/>Data Lake / Warehouse)]
    end

    subgraph Serving ["Serving Layer"]
        UI[Streamlit Dashboard<br/>Web Interface]
    end

    %% Định nghĩa luồng dữ liệu (Edges)
    PyGen -- "1. Sinh log User/Bot (JSON)" --> Kafka
    
    Kafka -- "2a. Đọc Stream liên tục" --> Flink
    Kafka -- "2b. Đọc Batch định kỳ" --> Spark
    
    Flink -- "3a. Phát hiện Bot / Aggregation" --> PG
    Spark -- "3b. Xử lý SCD Type 2 / Dimensions" --> Iceberg
    
    PG -- "4a. Truy vấn Real-time" --> UI
    Iceberg -- "4b. Truy vấn Lịch sử" --> UI

    %% Phân màu cho đẹp
    classDef gen fill:#f9f871,stroke:#333,stroke-width:2px,color:#000;
    classDef ing fill:#ffc75f,stroke:#333,stroke-width:2px,color:#000;
    classDef proc fill:#ff9671,stroke:#333,stroke-width:2px,color:#000;
    classDef store fill:#ff6f91,stroke:#333,stroke-width:2px,color:#000;
    classDef serv fill:#d65db1,stroke:#333,stroke-width:2px,color:#000;

    class PyGen gen;
    class Kafka ing;
    class Flink,Spark proc;
    class PG,Iceberg store;
    class UI serv;