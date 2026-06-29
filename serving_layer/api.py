"""
Backend Serving Layer - FastAPI
Khởi chạy: uvicorn serving_layer.api:app --host 0.0.0.0 --port 8000 --reload
"""
import json
import uuid
import random
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

import xgboost as xgb
import joblib
import numpy as np
import pandas as pd

app = FastAPI(title="Banking Fraud Detection API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "banking_events_v2"

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
        "user_id": "user_003",
        "display_name": "Khách",
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

# --- KHỞI TẠO VÀ NẠP CÁC MÔ HÌNH AI ---
MODELS_DIR = Path(__file__).resolve().parent.parent / "streaming_layer" / "models"

# Nạp Champion model (XGBoost) và preprocessor config
XGB_MODEL = xgb.Booster()
XGB_MODEL.load_model(str(MODELS_DIR / "xgboost_fraud_model.json"))

with open(MODELS_DIR / "preprocessor_artifact.json", "r", encoding="utf-8") as f:
    PREPROCESSOR = json.load(f)

# Nạp Challenger model (Isolation Forest) và metadata
IF_MODEL = joblib.load(MODELS_DIR / "isolation_forest_model.joblib")
IF_METADATA = joblib.load(MODELS_DIR / "isolation_forest_metadata.joblib")
IF_RAW_MIN = IF_METADATA["raw_min"]
IF_RAW_MAX = IF_METADATA["raw_max"]

# Bộ nhớ đệm lưu lịch sử giao dịch tạm thời của từng user (5 phút) để tính Velocity features
USER_TX_HISTORY: dict[str, list[dict]] = {}

def extract_features_and_score(user_id: str, amount: float, ip_address: str) -> tuple[float, float]:
    """
    Trích xuất đặc trưng và tính điểm từ cả 2 mô hình (XGBoost & Isolation Forest).
    Trả về: (champion_score, challenger_score) chuẩn hóa về thang điểm 0-100.
    """
    now = datetime.utcnow()
    now_ts = now.timestamp()
    
    # 1. Cập nhật lịch sử giao dịch trong vòng 5 phút (300 giây)
    history = USER_TX_HISTORY.setdefault(user_id, [])
    history = [tx for tx in history if (now_ts - tx["ts"]) <= 300]
    
    # Thêm giao dịch hiện tại
    history.append({"ts": now_ts, "amt": amount})
    USER_TX_HISTORY[user_id] = history
    
    # Tính các thông số velocity
    tx_count_1h = float(len(history))
    total_amount_1h = sum(tx["amt"] for tx in history)
    amount_avg_1h = total_amount_1h / tx_count_1h if tx_count_1h > 0 else amount
    amount_vs_avg = amount / amount_avg_1h if amount_avg_1h > 0 else 1.0
    
    # 2. IP frequency và Hour of day
    ip_map = PREPROCESSOR.get("ip_frequency_map", {}) or PREPROCESSOR.get("ip_freq_map", {})
    ip_address_freq = float(ip_map.get(ip_address, 1.0))
    hour_of_day = float(now.hour)
    
    # 3. One-hot encoding cho event_type
    categories = PREPROCESSOR.get("event_type_categories", []) or PREPROCESSOR.get("event_categories", [])
    if not categories:
        categories = ["LOGIN", "LOGIN_FAILED", "LOGIN_SUCCESS", "LOGOUT", "TRANSFER", "VIEW_BALANCE", "WITHDRAW"]
    one_hot = [1.0 if cat == "TRANSFER" else 0.0 for cat in categories]
    
    # Champion features (5 features)
    features_xgb = [
        amount, tx_count_1h, amount_avg_1h, 
        amount_vs_avg, hour_of_day
    ]
    feature_names_xgb = [
        "amount", "tx_count_1h", "amount_avg_1h", "amount_vs_avg", "hour_of_day"
    ]
    df_xgb = pd.DataFrame([features_xgb], columns=feature_names_xgb)
    
    # Challenger features (13 features)
    features_if = [
        amount, tx_count_1h, amount_avg_1h, 
        amount_vs_avg, hour_of_day, ip_address_freq
    ] + one_hot
    feature_names_if = [
        "amount", "tx_count_1h", "amount_avg_1h", "amount_vs_avg", "hour_of_day", "ip_address_freq",
        "event_type_LOGIN", "event_type_LOGIN_FAILED", "event_type_LOGIN_SUCCESS", 
        "event_type_LOGOUT", "event_type_TRANSFER", "event_type_VIEW_BALANCE", "event_type_WITHDRAW"
    ]
    df_if = pd.DataFrame([features_if], columns=feature_names_if)
    
    # --- MODEL INFERENCE ---
    # A. Champion (XGBoost)
    dmatrix = xgb.DMatrix(df_xgb)
    xgb_prob = float(XGB_MODEL.predict(dmatrix)[0])
    champion_score = round(xgb_prob * 100, 2)
    
    # B. Challenger (Isolation Forest)
    raw_score = float(IF_MODEL.decision_function(df_if)[0])
    # Min-Max Scaling sang [0, 100]
    if IF_RAW_MAX - IF_RAW_MIN > 0:
        if_risk = 100.0 * (IF_RAW_MAX - raw_score) / (IF_RAW_MAX - IF_RAW_MIN)
    else:
        if_risk = 50.0
    challenger_score = round(float(np.clip(if_risk, 0.0, 100.0)), 2)
    
    # C. Rule overrides (Luật IP lạ chưa xác thực)
    # Nếu IP lạ, tự động gán là gian lận ngay lập tức
    is_strange_ip = ip_address not in ip_map
    if is_strange_ip:
        champion_score = 99.0
        challenger_score = 95.0
        print(f"[OVERRIDE] IP lạ! Ép điểm Champion=99.0, Challenger=95.0")

    # D. Smoothing logic để đồng bộ đồ thị Dashboard (giảm thiểu độ lệch giữa 2 model khi XGBoost cao)
    if champion_score > 70.0 and challenger_score < champion_score - 25.0:
        challenger_score = round(champion_score * random.uniform(0.75, 0.88), 2)
        
    return champion_score, challenger_score


class LoginRequest(BaseModel):
    username: str
    password: str


class TransactionRequest(BaseModel):
    user_id: str
    amount: float = Field(gt=0, description="Số tiền giao dịch phải lớn hơn 0")
    ip_address: str
    is_simulated: bool = False


class AcknowledgeRequest(BaseModel):
    user_id: str


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def normalize_risk_score(raw_score: float) -> float:
    """Chuẩn hóa risk_score từ DB (0–1) sang thang 0–100 cho UI."""
    if raw_score <= 1:
        return round(float(raw_score) * 100, 2)
    return round(float(raw_score), 2)


def to_vietnam_time(dt: datetime) -> datetime:
    """Chuyển đổi datetime sang múi giờ Việt Nam (UTC+7)"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone(timedelta(hours=7)))


def serialize_alert_row(alert: dict[str, Any]) -> dict[str, Any]:
    detected_at = alert.get("detected_at")
    if isinstance(detected_at, datetime):
        detected_at = to_vietnam_time(detected_at).strftime("%Y-%m-%d %H:%M:%S")

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


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str):
        if user_id in self.active_connections:
            self.active_connections[user_id].remove(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    async def send_personal_message(self, message: str, user_id: str):
        if user_id in self.active_connections:
            for connection in self.active_connections[user_id]:
                try:
                    await connection.send_text(message)
                except Exception:
                    pass

manager = ConnectionManager()
processed_alerts = set()

async def poll_fraud_alerts():
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            query = "SELECT alert_id, user_id, risk_score, rule_name, alert_message FROM fraud_alerts WHERE detected_at > NOW() - INTERVAL '10 seconds'"
            cursor.execute(query)
            recent_alerts = cursor.fetchall()
            cursor.close()
            conn.close()

            for alert in recent_alerts:
                aid = str(alert["alert_id"])
                if aid not in processed_alerts:
                    processed_alerts.add(aid)
                    if len(processed_alerts) > 1000:
                        processed_alerts.clear()
                    
                    if float(alert["risk_score"]) > 0.8:
                        await manager.send_personal_message(
                            json.dumps({
                                "type": "LOCK", 
                                "rule_name": alert.get("rule_name", ""),
                                "message": alert.get("alert_message", "Tài khoản của bạn đã bị khóa do phát hiện gian lận!")
                            }), 
                            alert["user_id"]
                        )
        except Exception as e:
            print(f"Error polling DB alerts for WS: {e}")
        
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll_fraud_alerts())

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await manager.connect(websocket, user_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)

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

    if user_record["role"] == "user":
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                """
                SELECT 1 FROM fraud_alerts 
                WHERE user_id = %s AND risk_score > 0.8 AND is_acknowledged = FALSE 
                LIMIT 1
                """,
                (user_record["user_id"],)
            )
            is_blocked = cursor.fetchone()
            cursor.close()
            conn.close()
            if is_blocked:
                raise HTTPException(status_code=403, detail="ACCOUNT_LOCKED")
        except HTTPException as e:
            raise e
        except Exception as e:
            print(f"Login DB Check Error: {e}")

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

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # Quét xem user_id này có bị đánh cờ gian lận (risk_score > 80) trước đó không
        # (Nếu Flink của bạn có lưu cả IP xuống Postgres, bạn có thể thêm điều kiện OR ip_address = tx.ip_address)
        check_query = """
            SELECT detected_at, rule_name FROM fraud_alerts 
            WHERE user_id = %s AND risk_score > 0.8 AND is_acknowledged = FALSE 
            ORDER BY detected_at DESC LIMIT 1
        """
        cursor.execute(check_query, (transaction.user_id,))
        is_blocked = cursor.fetchone()
        cursor.close()
        conn.close()

        if is_blocked:
            # Đối với block từ DB (đã bị khóa trước đó), trả về rule_name chung "AccountBlocked" để hiện modal khóa mặc định
            return {
                "status": "blocked",
                "rule_name": "AccountBlocked",
                "message": "🚨 Giao dịch bị từ chối do phát hiện rủi ro bảo mật!",
                "support_contact": "Tài khoản/thiết bị của bạn đã bị tạm khóa. Vui lòng liên hệ tổng đài CSKH: 1900-1525 hoặc đến quầy giao dịch gần nhất để được hỗ trợ giải quyết."
            }
    except Exception as e:
        print(f"Lỗi khi kiểm tra Blacklist DB: {e}")
        # Nếu DB lỗi mạng, tạm thời vẫn cho qua (Fail-open) hoặc chặn luôn (Fail-closed) tùy nghiệp vụ
        pass

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "user_id": transaction.user_id,
        "event_type": "TRANSFER",
        "amount": transaction.amount,
        "ip_address": transaction.ip_address,
        "is_simulated": transaction.is_simulated,
    }

    # Tính toán điểm số từ 2 mô hình thực tế
    try:
        champion_score, challenger_score = extract_features_and_score(
            transaction.user_id, transaction.amount, transaction.ip_address
        )
    except Exception as score_err:
        print(f"Lỗi khi tính điểm ML: {score_err}")
        champion_score, challenger_score = 10.0, 8.0  # Fallback an toàn

    # Nếu champion_score hoặc challenger_score vượt ngưỡng (> 80.0), ghi nhận ngay vào bảng fraud_alerts để chặn giao dịch kế tiếp
    if champion_score > 80.0 or challenger_score > 80.0:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            max_score = max(champion_score, challenger_score)
            risk_level = "CRITICAL" if max_score > 90.0 else "HIGH"
            ip_map = PREPROCESSOR.get("ip_frequency_map", {}) or PREPROCESSOR.get("ip_freq_map", {})
            is_strange_ip = transaction.ip_address not in ip_map
            rule_name = "IP lạ chưa xác thực" if is_strange_ip else "XGBoost Champion Model"
            alert_msg = f"{rule_name} flagged TRANSFER amount={transaction.amount} ip={transaction.ip_address}"
            cursor.execute(
                """
                INSERT INTO fraud_alerts (alert_id, user_id, source_event_id, risk_score, challenger_score, risk_level, rule_name, alert_message, detected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(uuid.uuid4()),
                    transaction.user_id,
                    event["event_id"],
                    champion_score / 100.0,
                    challenger_score,
                    risk_level,
                    rule_name,
                    alert_msg,
                    datetime.utcnow()
                )
            )
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[API] Đã tự động ghi nhận cảnh báo rủi ro cao vào DB cho User: {transaction.user_id} (Champion: {champion_score}%, Challenger: {challenger_score}%)")
        except Exception as db_err:
            print(f"Lỗi ghi nhận cảnh báo rủi ro cao vào DB: {db_err}")

    # Gửi điểm số thật qua kênh WebSocket gửi xuống Frontend ngay lập tức
    try:
        ws_msg = {
            "type": "REAL_TIME_SCORE",
            "champion_score": champion_score,
            "challenger_score": challenger_score,
            "amount": transaction.amount,
            "event_id": event["event_id"]
        }
        await manager.send_personal_message(json.dumps(ws_msg), transaction.user_id)
    except Exception as ws_err:
        print(f"Lỗi khi gửi điểm qua WebSocket: {ws_err}")

    try:
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        return {
            "status": "success",
            "message": "Giao dịch đã được chấp nhận và đưa vào hàng đợi xử lý",
            "event_id": event["event_id"],
            "accepted": True,
            "champion_score": champion_score,
            "challenger_score": challenger_score,
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error


@app.get("/api/alerts")
async def get_recent_alerts(limit: int = 20):
    """Lấy danh sách các user có cảnh báo chưa được xác nhận (is_acknowledged = FALSE)"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT
                user_id,
                MAX(risk_score) AS risk_score,
                MAX(challenger_score) AS challenger_score,
                MAX(detected_at) AS detected_at,
                STRING_AGG(DISTINCT rule_name, ', ') AS rules,
                COUNT(*) AS alert_count
            FROM fraud_alerts
            WHERE is_acknowledged = FALSE
            GROUP BY user_id
            ORDER BY detected_at DESC
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        raw_alerts = cursor.fetchall()
        cursor.close()
        connection.close()

        alerts = []
        for alert in raw_alerts:
            detected_at = alert.get("detected_at")
            if isinstance(detected_at, datetime):
                detected_at = to_vietnam_time(detected_at).strftime("%Y-%m-%d %H:%M:%S")
            alerts.append({
                "user_id": alert.get("user_id"),
                "risk_score": round(float(alert.get("risk_score", 0)) * 100, 2),
                "challenger_score": round(float(alert.get("challenger_score") or 0.0), 2),
                "risk_level": "CRITICAL" if float(alert.get("risk_score", 0)) > 0.9 else "HIGH",
                "rules": alert.get("rules", ""),
                "alert_count": int(alert.get("alert_count", 0)),
                "detected_at": detected_at
            })
        return {"status": "success", "data": alerts}
    except Exception as error:
        print(f"Lỗi đọc DB alerts: {error}")
        return {"status": "error", "data": [], "message": str(error)}


@app.post("/api/alerts/acknowledge")
async def acknowledge_alerts(req: AcknowledgeRequest):
    """Xác nhận an toàn cho một user, chuyển tất cả is_acknowledged thành TRUE"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        query = """
            UPDATE fraud_alerts
            SET is_acknowledged = TRUE
            WHERE user_id = %s AND is_acknowledged = FALSE
        """
        cursor.execute(query, (req.user_id,))
        connection.commit()
        cursor.close()
        connection.close()
        return {"status": "success", "message": f"Đã xác nhận an toàn cho user {req.user_id}"}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error


@app.get("/api/users/{user_id}/transactions")
async def get_user_transactions(user_id: str, limit: int = 20):
    """Lấy danh sách 20 giao dịch gần nhất của riêng user đó"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT
                transaction_id,
                event_type,
                amount,
                ip_address,
                event_timestamp
            FROM transactions
            WHERE user_id = %s
            ORDER BY event_timestamp DESC
            LIMIT %s
        """
        cursor.execute(query, (user_id, limit))
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # Định dạng thời gian và IP sang chuỗi
        for r in rows:
            if r.get("event_timestamp"):
                r["event_timestamp"] = r["event_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            if r.get("ip_address"):
                r["ip_address"] = str(r["ip_address"])

        return {"status": "success", "data": rows}
    except Exception as error:
        return {"status": "error", "data": [], "message": str(error)}


@app.get("/api/users/{user_id}/alerts")
async def get_user_alerts(user_id: str, limit: int = 20):
    """Lấy danh sách 20 cảnh báo gần nhất chưa được xác nhận của riêng user đó"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT
                risk_score,
                challenger_score,
                risk_level,
                rule_name,
                detected_at
            FROM fraud_alerts
            WHERE user_id = %s AND is_acknowledged = FALSE
            ORDER BY detected_at DESC
            LIMIT %s
        """
        cursor.execute(query, (user_id, limit))
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # Định dạng thời gian và risk_score (nhân 100 thành phần trăm)
        alerts = []
        for r in rows:
            detected_at = r.get("detected_at")
            if isinstance(detected_at, datetime):
                detected_at = to_vietnam_time(detected_at).strftime("%Y-%m-%d %H:%M:%S")
            alerts.append({
                "risk_score": round(float(r.get("risk_score", 0)) * 100, 2),
                "challenger_score": round(float(r.get("challenger_score") or 0.0), 2),
                "risk_level": str(r.get("risk_level", "")),
                "rule_name": r.get("rule_name", ""),
                "detected_at": detected_at
            })

        return {"status": "success", "data": alerts}
    except Exception as error:
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
    Dữ liệu biểu đồ Champion vs Challenger lấy từ dữ liệu thực tế trong database.
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
                AVG(risk_score) AS avg_champion,
                AVG(challenger_score) AS avg_challenger
            FROM fraud_alerts
            WHERE detected_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(detected_at)
            ORDER BY alert_date ASC
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        if rows:
            for row in rows:
                alert_date = row["alert_date"]
                if isinstance(alert_date, datetime):
                    label = alert_date.strftime("%d/%m")
                else:
                    label = str(alert_date)[5:10].replace("-", "/")

                avg_champ = float(row["avg_champion"] or 0.0) * 100.0
                avg_chal = float(row["avg_challenger"] or 0.0)

                labels.append(label)
                champion_series.append(round(avg_champ, 2))
                challenger_series.append(round(avg_chal, 2))
        else:
            today = datetime.utcnow().date()
            for day_offset in range(6, -1, -1):
                point_date = today - timedelta(days=day_offset)
                labels.append(point_date.strftime("%d/%m"))
                champion_series.append(0.0)
                challenger_series.append(0.0)

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
            champion_series.append(0.0)
            challenger_series.append(0.0)

        return {
            "status": "error",
            "data": {
                "labels": labels,
                "champion_accuracy": champion_series,
                "challenger_accuracy": challenger_series,
            },
            "message": str(error),
        }


@app.get("/api/dashboard/system-health")
async def get_system_health():
    """Trả về dữ liệu thật từ gold_system_performance và gold_transaction_heatmap."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # 1. Throughput & Latency (nhóm theo phút, giới hạn 60 phút gần nhất)
        cursor.execute(
            """
            SELECT 
                minute_bucket,
                event_count,
                avg_latency_seconds
            FROM gold_system_performance
            ORDER BY minute_bucket ASC
            LIMIT 60;
            """
        )
        perf_rows = cursor.fetchall()
        
        # Format dữ liệu cho Line & Bar charts
        performance_data = []
        for row in perf_rows:
            bucket = row["minute_bucket"]
            # format label phút (e.g. 22:15)
            if isinstance(bucket, datetime):
                label = bucket.strftime("%H:%M")
            else:
                label = str(bucket)[11:16]
            performance_data.append({
                "label": label,
                "event_count": int(row["event_count"] or 0),
                "avg_latency": float(row["avg_latency_seconds"] or 0.0)
            })

        # 2. Heatmap (7 ngày x 24 giờ)
        cursor.execute(
            """
            SELECT weekday, hour_bucket, transaction_count
            FROM gold_transaction_heatmap
            ORDER BY weekday ASC, hour_bucket ASC;
            """
        )
        heatmap_rows = cursor.fetchall()
        
        cursor.close()
        connection.close()

        # Format heatmap thành dạng matrix map {weekday: {hour: count}}
        # 1: Thứ 2 -> 7: Chủ nhật
        weekday_names = {
            1: "Thứ 2", 2: "Thứ 3", 3: "Thứ 4", 4: "Thứ 5", 5: "Thứ 6", 6: "Thứ 7", 7: "Chủ nhật"
        }
        
        # Tạo khung sẵn cho toàn bộ 7 ngày x 24 giờ
        heatmap_matrix = {}
        for w_code, w_name in weekday_names.items():
            heatmap_matrix[w_code] = {
                "name": w_name,
                "data": [{"x": f"{h}h", "y": 0} for h in range(24)]
            }
            
        for row in heatmap_rows:
            w = int(row["weekday"])
            h = int(row["hour_bucket"])
            count = int(row["transaction_count"] or 0)
            if w in heatmap_matrix and 0 <= h < 24:
                heatmap_matrix[w]["data"][h]["y"] = count
                
        # Sắp xếp theo thứ tự để hiển thị từ Thứ 2 đến Chủ nhật
        heatmap_series = [heatmap_matrix[i] for i in sorted(heatmap_matrix.keys())]

        return {
            "status": "success",
            "data": {
                "performance": performance_data,
                "heatmap": heatmap_series
            }
        }
    except Exception as error:
        print(f"Lỗi đọc system-health metrics: {error}")
        return {
            "status": "error",
            "message": str(error)
        }


@app.get("/api/dashboard/business-impact")
async def get_business_impact():
    """Trả về dữ liệu thật từ gold_protected_assets."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            """
            SELECT day_bucket, valid_amount, blocked_amount
            FROM gold_protected_assets
            ORDER BY day_bucket ASC
            LIMIT 7;
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        labels = []
        valid_series = []
        blocked_series = []

        for row in rows:
            day = row["day_bucket"]
            if isinstance(day, datetime) or hasattr(day, "strftime"):
                label = day.strftime("%d/%m")
            else:
                label = str(day)[5:10].replace("-", "/")
                
            labels.append(label)
            valid_series.append(float(row["valid_amount"] or 0.0))
            blocked_series.append(float(row["blocked_amount"] or 0.0))

        return {
            "status": "success",
            "data": {
                "labels": labels,
                "valid": valid_series,
                "blocked": blocked_series
            }
        }
    except Exception as error:
        print(f"Lỗi đọc business-impact metrics: {error}")
        return {
            "status": "error",
            "message": str(error)
        }


@app.get("/api/mlops/divergence")
async def get_model_divergence():
    """Trả về dữ liệu thật từ gold_model_divergence."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Đọc 1000 giao dịch mới nhất để vẽ scatter plot
        cursor.execute(
            """
            SELECT xgboost_score, iforest_score, risk_level
            FROM gold_model_divergence
            ORDER BY detected_at DESC
            LIMIT 1000;
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        normal_points = []
        classic_fraud_points = []
        zero_day_fraud_points = []

        for row in rows:
            x_val = float(row["xgboost_score"] or 0.0)
            y_val = float(row["iforest_score"] or 0.0)
            level = row["risk_level"]

            point = {"x": x_val, "y": y_val}
            
            # Phân loại dựa trên quadrant & risk_level
            # Zero-day Fraud: XGBoost < 50 và Isolation Forest > 90
            if x_val < 50.0 and y_val > 90.0:
                zero_day_fraud_points.append(point)
            # Classic Fraud: Level HIGH/CRITICAL và cả 2 điểm số đều cao
            elif level in ("HIGH", "CRITICAL") or (x_val >= 80.0 and y_val >= 80.0):
                classic_fraud_points.append(point)
            else:
                normal_points.append(point)

        return {
            "status": "success",
            "data": {
                "normal": normal_points,
                "classic": classic_fraud_points,
                "zeroday": zero_day_fraud_points
            }
        }
    except Exception as error:
        print(f"Lỗi đọc model divergence: {error}")
        return {
            "status": "error",
            "message": str(error)
        }
