from kafka import KafkaConsumer
import json
from threading import Thread
import queue
from typing import Optional
from typing import Dict, Any


class StockDataConsumer:
    def __init__(self, topic: str, kafka_host: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[f"{kafka_host}:9092"],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            # enable_auto_commit=True,
            group_id=f"{topic}_group",
            fetch_max_wait_ms=300
        )
        self.data_queue = queue.Queue()
        self.start_consuming()

    def start_consuming(self) -> None:
        Thread(target=self._consume_messages, daemon=True).start()

    def _consume_messages(self) -> None:
        for message in self.consumer:
            self.data_queue.put(message.value)

    def get_latest_data(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        try:
            return self.data_queue.get(timeout=timeout)
        except queue.Empty:
            return None
