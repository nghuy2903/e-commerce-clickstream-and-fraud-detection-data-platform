"""
Tầng 3 — Luồng Real-time (PyFlink): Stateful Fraud Detection
Sử dụng KeyedProcessFunction để đếm Spam và nhận diện IP lạ động.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy, Row
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

import xgboost as xgb
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("fraud_detector")

# --- CẤU HÌNH MÔI TRƯỜNG ---
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "banking_events_v2")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "flink-fraud-detector")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "banking_mlops")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin123")

CHECKPOINT_INTERVAL_MS = int(os.environ.get("FLINK_CHECKPOINT_INTERVAL_MS", "60000"))
WATERMARK_MAX_OUT_OF_ORDER_SEC = int(os.environ.get("FLINK_WATERMARK_MAX_OUT_OF_ORDER_SEC", "30"))

@dataclass(frozen=True)
class BankingEvent:
    event_id: str
    timestamp: str
    user_id: str
    event_type: str
    amount: float
    ip_address: str
    is_simulated: bool

@dataclass(frozen=True)
class FraudAlertRecord:
    source_event_id: str
    user_id: str
    risk_score: float
    risk_level: str
    rule_name: str
    alert_message: str
    detected_at: str

def parse_banking_event(raw_json: str) -> BankingEvent | None:
    try:
        payload = json.loads(raw_json)
        return BankingEvent(
            event_id=str(payload["event_id"]),
            timestamp=str(payload["timestamp"]),
            user_id=str(payload["user_id"]),
            event_type=str(payload["event_type"]),
            amount=float(payload.get("amount", 0.0)),
            ip_address=str(payload.get("ip_address", "0.0.0.0")),
            is_simulated=bool(payload.get("is_simulated", False)),
        )
    except Exception as exc:
        logger.warning(f"Parse lỗi: {exc}")
        return None

def _risk_level_from_score(score_0_100: float) -> str:
    if score_0_100 > 90: return "CRITICAL"
    if score_0_100 > 80: return "HIGH"
    if score_0_100 > 50: return "MEDIUM"
    return "LOW"

# --- BƯỚC 1: MAPPER GIẢI MÃ JSON ---
class ParseEventMapper(MapFunction):
    def map(self, value: str) -> BankingEvent | None:
        return parse_banking_event(value)

# --- BƯỚC 2: STATEFUL PROCESS FUNCTION (CÓ BỘ NHỚ) ---
class FraudScoringProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        # 1. Khởi tạo XGBoost & Preprocessor
        self._fraud_threshold = 0.8
        model_dir = Path("/tmp/models")
        self.model = xgb.Booster()
        self.model.load_model(str(model_dir / "xgboost_fraud_model.json"))
        
        with open(model_dir / "preprocessor_artifact.json", "r", encoding="utf-8") as f:
            self.preprocessor = json.load(f)
            
        # 2. Khởi tạo StateBackend (Bộ nhớ đệm lưu lịch sử giao dịch)
        # Thiết lập TTL: Dữ liệu tự động bốc hơi sau 1 giờ để giải phóng RAM
        ttl_config = StateTtlConfig.new_builder(Time.hours(1)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .build()
            
        state_desc = ValueStateDescriptor("tx_history", Types.STRING())
        state_desc.enable_time_to_live(ttl_config)
        self.history_state = runtime_context.get_state(state_desc)

    def process_element(self, event: BankingEvent, ctx: KeyedProcessFunction.Context) -> Iterable[FraudAlertRecord]:
        logger.info(f"[RECEIVED] Xử lý event {event.event_id} từ {event.user_id}")
        
        # --- LOGIC 1: ĐẾM GIAO DỊCH (STATEFUL SPAM DETECTION) ---
        now_ts = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00")).timestamp()
        
        # Lấy lịch sử từ bộ nhớ (List các dict chứa timestamp và amount)
        history_str = self.history_state.value()
        history = json.loads(history_str) if history_str else []
        
        # Lọc bỏ các giao dịch cũ hơn 1 giờ (3600 giây)
        history = [tx for tx in history if (now_ts - tx["ts"]) <= 300]
        
        # Thêm giao dịch hiện tại vào bộ nhớ
        history.append({"ts": now_ts, "amt": float(event.amount)})
        self.history_state.update(json.dumps(history))
        
        # Tính toán thông số thực tế từ bộ nhớ
        tx_count_1h = float(len(history))
        total_amount_1h = sum(tx["amt"] for tx in history)
        amount_avg_1h = total_amount_1h / tx_count_1h
        amount_vs_avg = float(event.amount) / amount_avg_1h if amount_avg_1h > 0 else 1.0

        # --- LOGIC 2: NHẬN DIỆN MỌI IP LẠ (DYNAMIC RULE) ---
        known_ips = self.preprocessor.get("ip_freq_map", {})
        is_strange_ip = event.ip_address not in known_ips
        ip_address_freq = float(known_ips.get(event.ip_address, 1.0))
        
        if is_strange_ip:
            logger.warning(f"[RULE-BASED] IP hoàn toàn mới: '{event.ip_address}'")
            # Ép điểm 99% nếu IP lạ VÀ đang có dấu hiệu spam (>= 3 giao dịch/giờ)
            if tx_count_1h >= 3:
                yield self._create_alert(event, 99.0, "Spam từ IP lạ")
                return

        # --- LOGIC 3: XGBoost SCORING ---
        hour_of_day = float(datetime.fromisoformat(event.timestamp.replace("Z", "+00:00")).hour)
        expected_categories = [
            "LOGIN", "LOGIN_FAILED", "LOGIN_SUCCESS", 
            "LOGOUT", "TRANSFER", "VIEW_BALANCE", "WITHDRAW"
        ]
        
        # Tạo One-Hot vector có độ dài cố định là 7
        one_hot = [1.0 if event.event_type == cat else 0.0 for cat in expected_categories]
        
        # Ráp chuẩn xác 13 cột (6 cột số + 7 cột sự kiện)
        features = [
            float(event.amount), tx_count_1h, amount_avg_1h, 
            amount_vs_avg, hour_of_day, ip_address_freq
        ] + one_hot
        
        feature_names = [
            "amount", "tx_count_1h", "amount_avg_1h", "amount_vs_avg", "hour_of_day", "ip_address_freq",
            "event_type_LOGIN", "event_type_LOGIN_FAILED", "event_type_LOGIN_SUCCESS", 
            "event_type_LOGOUT", "event_type_TRANSFER", "event_type_VIEW_BALANCE", "event_type_WITHDRAW"
        ]
        
        # Inference an toàn
        dmatrix = xgb.DMatrix(pd.DataFrame([features], columns=feature_names))
        risk_score = float(self.model.predict(dmatrix)[0])
        
        logger.warning(f"[INFERENCE] User: {event.user_id} | TX_1h: {tx_count_1h} | Lạ IP: {is_strange_ip} | Score: {risk_score:.4f}")
        
        if risk_score > self._fraud_threshold:
            yield self._create_alert(event, risk_score * 100, "XGBoost Champion Model")

    def _create_alert(self, event: BankingEvent, score_0_100: float, rule: str) -> FraudAlertRecord:
        risk_level = _risk_level_from_score(score_0_100)
        logger.warning(f"[FRAUD] Báo động! event={event.event_id} | score={score_0_100:.2f} | rule={rule}")
        return FraudAlertRecord(
            source_event_id=event.event_id,
            user_id=event.user_id,
            risk_score=round(score_0_100 / 100.0, 4),
            risk_level=risk_level,
            rule_name=rule,
            alert_message=f"{rule} flagged {event.event_type} amount={event.amount} ip={event.ip_address}",
            detected_at=datetime.now(timezone.utc).isoformat(),
        )

# --- CÁC COMPONENT GHI DATABASE ---
class FraudAlertToJdbcRowMapper(MapFunction):
    def map(self, alert: FraudAlertRecord) -> Row:
        return Row(str(uuid.uuid4()), alert.user_id, alert.source_event_id, alert.risk_score, alert.risk_level, alert.rule_name, alert.alert_message, alert.detected_at)

def _build_jdbc_sink() -> JdbcSink:
    sql = """INSERT INTO fraud_alerts (alert_id, user_id, source_event_id, risk_score, risk_level, rule_name, alert_message, detected_at) VALUES (?::uuid, ?, ?::uuid, ?, ?::fraud_risk_level, ?, ?, ?::timestamptz)"""
    return JdbcSink.sink(
        sql=sql,
        type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]),
        jdbc_connection_options=JdbcConnectionOptions.JdbcConnectionOptionsBuilder().with_url(f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}").with_driver_name("org.postgresql.Driver").with_user_name(POSTGRES_USER).with_password(POSTGRES_PASSWORD).build(),
        jdbc_execution_options=JdbcExecutionOptions.builder().with_batch_interval_ms(2000).with_batch_size(50).with_max_retries(3).build(),
    )

def build_pipeline(env: StreamExecutionEnvironment | None = None) -> StreamExecutionEnvironment:
    if env is None: env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    config = Configuration()
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    env.configure(config)
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder().set_bootstrap_servers(KAFKA_BOOTSTRAP).set_topics(KAFKA_TOPIC).set_group_id(KAFKA_GROUP_ID).set_starting_offsets(KafkaOffsetsInitializer.latest()).set_value_only_deserializer(SimpleStringSchema()).build()

    # KIẾN TRÚC PIPELINE MỚI
    raw_stream = env.from_source(kafka_source, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_MAX_OUT_OF_ORDER_SEC)), "kafka-source")
    
    alert_stream = (
        raw_stream
        .map(ParseEventMapper(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
        .key_by(lambda e: e.user_id, key_type=Types.STRING()) # Phân luồng theo User để tạo State
        .process(FraudScoringProcessFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .map(FraudAlertToJdbcRowMapper(), output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]))
    )

    alert_stream.add_sink(_build_jdbc_sink()).name("postgres-fraud-alerts")
    return env

def main() -> None:
    print(f"[STARTUP] Flink Fraud Detector Stateful | kafka={KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}")
    env = build_pipeline()
    env.execute("banking-fraud-detector-stateful")

if __name__ == "__main__":
    main()