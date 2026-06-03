
from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka import Producer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "banking_events"

NORMAL_USER_RATIO = 0.80
FRAUD_BOT_RATIO = 0.20

FRAUD_BOT_IPS = ("192.168.1.99", "10.0.0.88", "203.0.113.42")

# Markov: state -> [(next_state, weight), ...]
NORMAL_TRANSITIONS: dict[str, list[tuple[str, float]]] = {
    "LOGIN": [("VIEW_BALANCE", 1.0)],
    "VIEW_BALANCE": [("TRANSFER", 0.5), ("WITHDRAW", 0.5)],
    "TRANSFER": [("LOGOUT", 1.0)],
    "WITHDRAW": [("LOGOUT", 1.0)],
}

FRAUD_TRANSITIONS: dict[str, list[tuple[str, float]]] = {
    "LOGIN_FAILED": [("LOGIN_FAILED", 0.75), ("LOGIN_SUCCESS", 0.25)],
    "LOGIN_SUCCESS": [("TRANSFER", 1.0)],
    "TRANSFER": [("TRANSFER", 0.65), ("LOGOUT", 0.35)],
}


class MarkovChain:
    """Chuỗi Markov cho luồng trạng thái hành vi người dùng."""

    def __init__(
        self,
        transitions: dict[str, list[tuple[str, float]]],
        initial_state: str,
    ) -> None:
        self._transitions = transitions
        self.initial_state = initial_state

    def next_state(self, current: str) -> str:
        options = self._transitions[current]
        states, weights = zip(*options)
        return random.choices(states, weights=weights, k=1)[0]


class BankingSimulator:
    """Mô phỏng hành vi ngân hàng Normal_User (80%) và Fraud_Bot (20%)."""

    def __init__(
        self,
        broker: str = KAFKA_BROKER,
        topic: str = TOPIC_NAME,
        fraud_ratio: float = FRAUD_BOT_RATIO,
    ) -> None:
        self._broker = broker
        self._topic = topic
        self._fraud_ratio = fraud_ratio
        self._normal_chain = MarkovChain(NORMAL_TRANSITIONS, "LOGIN")
        self._fraud_chain = MarkovChain(FRAUD_TRANSITIONS, "LOGIN_FAILED")
        self._producer: Producer | None = None

    def _create_producer(self) -> Producer:
        return Producer({"bootstrap.servers": self._broker})

    @staticmethod
    def _utc_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _random_normal_ip() -> str:
        return f"172.16.{random.randint(0, 255)}.{random.randint(0, 255)}"

    def _new_user_id(self, is_fraud: bool) -> str:
        if is_fraud:
            return f"fraud_bot_{random.randint(1, 50)}"
        return f"normal_user_{random.randint(100, 9999)}"

    def _new_ip(self, is_fraud: bool) -> str:
        if is_fraud:
            return random.choice(FRAUD_BOT_IPS)
        return self._random_normal_ip()

    @staticmethod
    def _amount_for_event(event_type: str, is_fraud: bool) -> float:
        if event_type not in ("TRANSFER", "WITHDRAW"):
            return 0.0
        if is_fraud:
            # Số tiền bất thường: rất lớn hoặc lặp mẫu đáng ngờ
            if random.random() < 0.5:
                return round(random.uniform(50_000, 500_000), 2)
            return float(random.choice([9_999.99, 49_999.99, 99_999.99, 999_999.0]))
        if event_type == "WITHDRAW":
            return round(random.uniform(20, 2_000), 2)
        return round(random.uniform(50, 5_000), 2)

    def _build_event(
        self,
        event_type: str,
        user_id: str,
        ip_address: str,
        is_fraud: bool,
    ) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._utc_timestamp(),
            "user_id": user_id,
            "event_type": event_type,
            "amount": self._amount_for_event(event_type, is_fraud),
            "ip_address": ip_address,
            "is_simulated": True,
        }

    def _pick_is_fraud(self) -> bool:
        return random.random() < self._fraud_ratio

    def _run_session(self, is_fraud: bool, send: Callable[[dict], None]) -> None:
        chain = self._fraud_chain if is_fraud else self._normal_chain
        user_id = self._new_user_id(is_fraud)
        ip_address = self._new_ip(is_fraud)
        state = chain.initial_state

        while True:
            event = self._build_event(state, user_id, ip_address, is_fraud)
            send(event)
            if state == "LOGOUT":
                break
            state = chain.next_state(state)

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            print(f"Kafka delivery failed: {err}")

    def _send_event(self, event: dict) -> None:
        if self._producer is None:
            raise RuntimeError("Producer chưa được khởi tạo.")
        payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
        self._producer.produce(
            self._topic,
            value=payload,
            callback=self._delivery_callback,
        )
        self._producer.poll(0)
        print(f"Sent event: {json.dumps(event, ensure_ascii=False)}")

    def run(
        self,
        min_delay_sec: float = 0.05,
        max_delay_sec: float = 0.35,
    ) -> None:
        """Chạy vòng lặp sinh session và đẩy sự kiện lên Kafka."""
        try:
            self._producer = self._create_producer()
            print(
                f"Bắt đầu Banking Data Simulator — topic '{self._topic}' "
                f"tại {self._broker} (Normal {NORMAL_USER_RATIO:.0%} / "
                f"Fraud {self._fraud_ratio:.0%})..."
            )
        except Exception as exc:
            print(f"Không thể khởi tạo Kafka Producer: {exc}")
            return

        try:
            while True:
                is_fraud = self._pick_is_fraud()
                self._run_session(is_fraud, self._send_event)
                time.sleep(random.uniform(min_delay_sec, max_delay_sec))
        except KeyboardInterrupt:
            print("\nĐã nhận lệnh dừng simulator.")
        finally:
            if self._producer is not None:
                self._producer.flush()
                print("Đã flush và đóng kết nối Kafka an toàn.")


if __name__ == "__main__":
    simulator = BankingSimulator()
    simulator.run()
