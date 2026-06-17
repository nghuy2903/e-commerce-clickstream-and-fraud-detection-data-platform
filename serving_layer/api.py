"""
Backend Serving Layer - FastAPI
Khởi chạy: uvicorn serving_layer.api:app --host 0.0.0.0 --port 8000 --reload
"""
import json
import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Banking Fraud Detection API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "banking_events"

producer: KafkaProducer | None = None
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    print("Đã kết nối Kafka Producer")
except Exception as error:
    print(f"Chưa kết nối được Kafka: {error}")

DB_CONFIG = {
    "dbname": "banking_mlops",
    "user": "admin",
    "password": "admin123",
    "host": "localhost",
    "port": "5432",
}

DEMO_USERS: dict[str, dict[str, str]] = {
    "customer1": {
        "password": "password",
        "role": "user",
        "user_id": "user_999",
        "display_name": "Nguyễn Văn Khách",
    },
    "admin1": {
        "password": "password",
        "role": "admin",
        "user_id": "admin",
        "display_name": "Quản trị viên Hệ thống",
    },
}

ACTIVE_SESSIONS: dict[str, dict[str, Any]] = {}

SERVING_DIR = Path(__file__).resolve().parent


class LoginRequest(BaseModel):
    username: str
    password: str


class TransactionRequest(BaseModel):
    user_id: str
    amount: float = Field(gt=0, description="Số tiền giao dịch phải lớn hơn 0")
    ip_address: str
    is_simulated: bool = False


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def normalize_risk_score(raw_score: float) -> float:
    """Chuẩn hóa risk_score từ DB (0–1) sang thang 0–100 cho UI."""
    if raw_score <= 1:
        return round(float(raw_score) * 100, 2)
    return round(float(raw_score), 2)


def serialize_alert_row(alert: dict[str, Any]) -> dict[str, Any]:
    detected_at = alert.get("detected_at")
    if isinstance(detected_at, datetime):
        detected_at = detected_at.strftime("%Y-%m-%d %H:%M:%S")

    risk_score_display = normalize_risk_score(float(alert.get("risk_score", 0)))

    return {
        "alert_id": str(alert.get("alert_id", "")),
        "user_id": alert.get("user_id", ""),
        "risk_score": risk_score_display,
        "risk_level": alert.get("risk_level", "LOW"),
        "rule_name": alert.get("rule_name", ""),
        "alert_message": alert.get("alert_message", ""),
        "detected_at": detected_at,
        "is_high_risk": risk_score_display > 80,
    }


@app.get("/")
async def serve_index():
    """Phục vụ SPA tại root URL."""
    index_path = SERVING_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Không tìm thấy index.html")
    return FileResponse(index_path)


@app.post("/api/auth/login")
async def login(credentials: LoginRequest):
    """Xác thực tài khoản demo và cấp token phiên làm việc."""
    username = credentials.username.strip()
    password = credentials.password

    user_record = DEMO_USERS.get(username)
    if user_record is None or user_record["password"] != password:
        raise HTTPException(status_code=401, detail="Tên đăng nhập hoặc mật khẩu không đúng")

    token = str(uuid.uuid4())
    session_payload = {
        "username": username,
        "role": user_record["role"],
        "user_id": user_record["user_id"],
        "display_name": user_record["display_name"],
        "issued_at": datetime.utcnow().isoformat() + "Z",
    }
    ACTIVE_SESSIONS[token] = session_payload

    return {
        "status": "success",
        "token": token,
        "username": username,
        "role": user_record["role"],
        "user_id": user_record["user_id"],
        "display_name": user_record["display_name"],
    }


@app.post("/api/auth/logout")
async def logout(payload: dict[str, str]):
    """Huỷ token phiên làm việc."""
    token = payload.get("token")
    if token and token in ACTIVE_SESSIONS:
        del ACTIVE_SESSIONS[token]
    return {"status": "success", "message": "Đã đăng xuất"}


@app.get("/api/auth/demo-accounts")
async def get_demo_accounts():
    """Trả về danh sách tài khoản demo (không bao gồm mật khẩu)."""
    accounts = []
    for username, profile in DEMO_USERS.items():
        accounts.append(
            {
                "username": username,
                "role": profile["role"],
                "user_id": profile["user_id"],
                "display_name": profile["display_name"],
            }
        )
    return {"status": "success", "data": accounts}


@app.post("/api/transactions")
async def create_transaction(transaction: TransactionRequest):
    """Nhận giao dịch từ SPA và đẩy vào Kafka."""
    if producer is None:
        raise HTTPException(
            status_code=503,
            detail="Kafka Producer chưa sẵn sàng. Vui lòng kiểm tra broker.",
        )

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "user_id": transaction.user_id,
        "event_type": "TRANSFER",
        "amount": transaction.amount,
        "ip_address": transaction.ip_address,
        "is_simulated": transaction.is_simulated,
    }

    try:
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        return {
            "status": "success",
            "message": "Giao dịch đã được chấp nhận và đưa vào hàng đợi xử lý",
            "event_id": event["event_id"],
            "accepted": True,
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error


@app.get("/api/alerts")
async def get_recent_alerts(limit: int = 20):
    """Polling endpoint — lấy cảnh báo gian lận mới nhất từ PostgreSQL."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT
                alert_id,
                user_id,
                risk_score,
                risk_level,
                rule_name,
                alert_message,
                detected_at
            FROM fraud_alerts
            ORDER BY detected_at DESC
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        raw_alerts = cursor.fetchall()
        cursor.close()
        connection.close()

        alerts = [serialize_alert_row(dict(alert)) for alert in raw_alerts]
        return {"status": "success", "data": alerts}
    except Exception as error:
        print(f"Lỗi đọc DB alerts: {error}")
        return {"status": "error", "data": [], "message": str(error)}


@app.get("/api/dashboard/stats")
async def get_dashboard_stats():
    """Thống kê tổng quan cho Admin Dashboard."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT COUNT(*) AS total FROM fraud_alerts")
        total_alerts = int(cursor.fetchone()["total"])

        cursor.execute(
            """
            SELECT COUNT(*) AS high_risk_count
            FROM fraud_alerts
            WHERE risk_score > 0.8 OR risk_level IN ('HIGH', 'CRITICAL')
            """
        )
        high_risk_count = int(cursor.fetchone()["high_risk_count"])

        cursor.execute(
            """
            SELECT COUNT(*) AS recent_count
            FROM fraud_alerts
            WHERE detected_at >= NOW() - INTERVAL '24 hours'
            """
        )
        recent_alerts = int(cursor.fetchone()["recent_count"])

        cursor.close()
        connection.close()

        fraud_rate = round((high_risk_count / total_alerts) * 100, 2) if total_alerts else 0.0
        pipeline_status = "live" if producer is not None else "degraded"

        return {
            "status": "success",
            "data": {
                "total_alerts": total_alerts,
                "high_risk_alerts": high_risk_count,
                "recent_alerts_24h": recent_alerts,
                "fraud_rate_percent": fraud_rate,
                "pipeline_status": pipeline_status,
            },
        }
    except Exception as error:
        print(f"Lỗi đọc dashboard stats: {error}")
        return {
            "status": "error",
            "data": {
                "total_alerts": 0,
                "high_risk_alerts": 0,
                "recent_alerts_24h": 0,
                "fraud_rate_percent": 0.0,
                "pipeline_status": "offline",
            },
            "message": str(error),
        }


@app.get("/api/mlops/metrics")
async def get_mlops_metrics():
    """
    Dữ liệu biểu đồ Champion vs Challenger.
    Ưu tiên tính từ fraud_alerts; fallback sang chuỗi mô phỏng ổn định nếu DB trống.
    """
    labels: list[str] = []
    champion_series: list[float] = []
    challenger_series: list[float] = []

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT
                DATE(detected_at) AS alert_date,
                COUNT(*) AS alert_count,
                AVG(risk_score) AS avg_risk
            FROM fraud_alerts
            WHERE detected_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(detected_at)
            ORDER BY alert_date ASC
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        base_champion = 94.2
        base_challenger = 91.5

        if rows:
            for index, row in enumerate(rows):
                alert_date = row["alert_date"]
                if isinstance(alert_date, datetime):
                    label = alert_date.strftime("%d/%m")
                else:
                    label = str(alert_date)[5:10].replace("-", "/")

                alert_count = int(row["alert_count"] or 0)
                avg_risk = float(row["avg_risk"] or 0.5)
                drift = min(alert_count * 0.08, 2.5)

                champion_accuracy = round(base_champion - (avg_risk * 3) - drift + index * 0.15, 2)
                challenger_accuracy = round(base_challenger - (avg_risk * 4) - drift + index * 0.1, 2)

                labels.append(label)
                champion_series.append(max(85.0, min(99.5, champion_accuracy)))
                challenger_series.append(max(82.0, min(97.0, challenger_accuracy)))

        if not labels:
            today = datetime.utcnow().date()
            for day_offset in range(6, -1, -1):
                point_date = today - timedelta(days=day_offset)
                labels.append(point_date.strftime("%d/%m"))
                champion_series.append(round(93.5 + random.uniform(-0.8, 1.2), 2))
                challenger_series.append(round(90.8 + random.uniform(-1.0, 1.0), 2))

        return {
            "status": "success",
            "data": {
                "labels": labels,
                "champion_accuracy": champion_series,
                "challenger_accuracy": challenger_series,
            },
        }
    except Exception as error:
        print(f"Lỗi đọc MLOps metrics: {error}")
        today = datetime.utcnow().date()
        for day_offset in range(6, -1, -1):
            point_date = today - timedelta(days=day_offset)
            labels.append(point_date.strftime("%d/%m"))
            champion_series.append(round(93.0 + day_offset * 0.2, 2))
            challenger_series.append(round(90.0 + day_offset * 0.15, 2))

        return {
            "status": "fallback",
            "data": {
                "labels": labels,
                "champion_accuracy": champion_series,
                "challenger_accuracy": challenger_series,
            },
            "message": str(error),
        }
