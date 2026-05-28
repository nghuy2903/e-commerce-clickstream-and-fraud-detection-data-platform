# Docker Layer

This compose setup is optimized for a physical host with 8GB RAM:
- Kafka in KRaft mode (no Zookeeper)
- Flink standalone single-node (JobManager + 1-slot TaskManager)
- PostgreSQL with constrained memory settings
- Streamlit lightweight serving app

Iceberg should use Hadoop catalog and write data directly to local filesystem
through the mounted path: `/opt/iceberg/warehouse`.
