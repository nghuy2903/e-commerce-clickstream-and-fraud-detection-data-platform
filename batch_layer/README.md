# Batch Layer

PySpark jobs for loading Kafka data into Apache Iceberg (Hadoop catalog)
on local filesystem and running analytical transformations.

| File | Mô tả |
|------|-------|
| `jobs/init_iceberg_tables.py` | Tạo `raw_banking_events`, `account_history` |
| `jobs/process_account_scd.py` | SCD Type 2 balance + MLOps model evaluation |
| `modules/account_history_scd.py` | MERGE + INSERT pattern cho SCD2 |

Xem hướng dẫn chi tiết: [`processing_layer/README.md`](../processing_layer/README.md).
