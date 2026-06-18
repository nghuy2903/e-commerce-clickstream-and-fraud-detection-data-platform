"""
Công cụ xuất dữ liệu từ Iceberg Warehouse ra file CSV để dễ dàng theo dõi.
"""
import sys
from pathlib import Path

# Đảm bảo import được thư viện của project
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark

def main():
    print("🚀 Đang kết nối vào Iceberg Data Lakehouse...")
    spark = build_iceberg_spark(app_name="export_iceberg_data")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Truy vấn 100 giao dịch mới nhất từ bảng Raw
        print("📥 Đang hút dữ liệu từ bảng local.raw.raw_banking_events...")
        df_raw = spark.sql("""
            SELECT  event_type, COUNT(*) AS total_category
            FROM local.raw.raw_banking_events 
            GROUP BY event_type
        """)
        
        # 2. Hiển thị dữ liệu trên Terminal bằng lệnh chuẩn của Spark
        if df_raw.isEmpty():
            print("⚠️ Bảng hiện tại chưa có dữ liệu nào.")
            return
            
        print("\n=== 10 GIAO DỊCH MỚI NHẤT ===")
        # truncate=False giúp hiển thị full text không bị cắt xén (dấu ...)
        df_raw.show(10, truncate=False)

        # 3. Xuất ra file CSV bằng bộ máy của Spark (Native)
        export_dir = "/app/batch_layer/warehouse/latest_raw_events_export"
        
        # coalesce(1) ép Spark gom toàn bộ dữ liệu phân tán vào đúng 1 file CSV duy nhất
        df_raw.coalesce(1).write.mode("overwrite").option("header", "true").csv(export_dir)
        
        print(f"\n✅ Đã xuất dữ liệu thành công ra thư mục vật lý tại:")
        print(f"👉 batch_layer/warehouse/latest_raw_events_export/")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()