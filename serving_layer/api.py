"""
Backend Serving Layer - FastAPI
Khởi chạy: uvicorn serving_layer.api:app --host 0.0.0.0 --port 8000 --reload
"""
import json
import uuid
import random
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Banking Fraud Detection API")

# Cấp quyền CORS để frontend HTML có thể gọi API thoải mái
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cấu hình Kafka Producer
KAFKA_BROKER = "localhost:9092" # Hoặc localhost:9092 nếu chạy ngoài Docker
KAFKA_TOPIC = "banking_events"

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(" Đã kết nối Kafka Producer")
except Exception as e:
    print(f" Chưa kết nối được Kafka: {e}")

# Cấu hình PostgreSQL (Sửa lại thông tin cho khớp với database của bạn)
DB_CONFIG = {
    "dbname": "banking_mlops",
    "user": "admin",
    "password": "admin123",
    "host": "localhost", # Hoặc localhost nếu chạy ngoài Docker
    "port": "5432"
}

# --- Pydantic Models ---
class TransactionRequest(BaseModel):
    user_id: str
    amount: float
    ip_address: str

# --- Endpoints ---
@app.post("/api/transactions")
async def create_transaction(tx: TransactionRequest):
    """Endpoint nhận giao dịch từ Web và đẩy vào Kafka"""
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "user_id": tx.user_id,
        "event_type": "TRANSFER",
        "amount": tx.amount,
        "ip_address": tx.ip_address,
        "is_simulated": False
    }
    
    try:
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        return {"status": "success", "message": "Đã gửi giao dịch vào hàng đợi", "event_id": event["event_id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/alerts")
async def get_recent_alerts(limit: int = 10):
    """Endpoint Polling để lấy cảnh báo gian lận mới nhất từ Postgres"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # Truy vấn bảng mà Flink đang sink data vào (Giả định tên bảng là fraud_alerts)
        query = """
            SELECT alert_id, user_id, risk_score, detected_at
            FROM fraud_alerts 
            ORDER BY detected_at DESC 
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        alerts = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Format lại datetime để JSON serialize được
        for alert in alerts:
            if isinstance(alert['detected_at'], datetime):
                alert['detected_at'] = alert['detected_at'].strftime("%Y-%m-%d %H:%M:%S")
                
        return {"status": "success", "data": alerts}
    except Exception as e:
        print(f"Lỗi đọc DB: {e}")
        # Trả về data ảo nếu chưa setup xong DB để UI không bị sập
        return {"status": "error", "data": [], "message": str(e)}