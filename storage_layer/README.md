# Storage Layer (Tầng 4)

## PostgreSQL

- Script: `postgres/01_init_banking_schema.sql`
- Tự chạy khi `docker compose up` với volume `postgres_data` mới (mount vào `/docker-entrypoint-initdb.d`).
- Kết nối: `postgresql://admin:admin123@localhost:5432/banking_mlops`

Nếu volume đã tồn tại, áp dụng thủ công:

```bash
docker exec -i postgres psql -U admin -d banking_mlops < storage_layer/postgres/01_init_banking_schema.sql
```

## Apache Iceberg (PySpark)

```bash
docker exec spark-master spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \
  /app/batch_layer/jobs/init_iceberg_tables.py
```

Warehouse mặc định: `batch_layer/warehouse/` (biến môi trường `ICEBERG_WAREHOUSE` để ghi đè).
