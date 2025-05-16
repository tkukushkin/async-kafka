from collections.abc import Callable, Coroutine, Mapping, Sequence
from datetime import datetime
from typing import Any

import anyio
import anyio.from_thread
import anyio.to_thread
import confluent_kafka
import confluent_kafka.admin


class Producer:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._producer = confluent_kafka.Producer(dict(config))

    def __len__(self) -> int:
        return len(self._producer)

    async def abort_transaction(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(self._producer.abort_transaction, timeout)

    def begin_transaction(self) -> None:
        self._producer.begin_transaction()

    async def commit_transaction(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(self._producer.commit_transaction, timeout)

    async def flush(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(self._producer.flush, timeout)

    async def init_transactions(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(self._producer.init_transactions, timeout)

    async def list_topics(
        self, topic: str | None = None, timeout: float | None = None
    ) -> confluent_kafka.admin.ClusterMetadata:
        if timeout is None:
            timeout = -1
        return await anyio.to_thread.run_sync(self._producer.list_topics, topic, timeout)

    async def poll(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(self._producer.poll, timeout)

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        *,
        key: str | bytes | None = None,
        partition: int | None = None,
        on_delivery: (
            Callable[[confluent_kafka.KafkaError | None, confluent_kafka.Message], Coroutine[Any, Any, Any]] | None
        ) = None,
        timestamp: int | datetime | None = None,
        headers: Mapping[str, str | bytes | None] | Sequence[tuple[str, str | bytes | None]] | None = None,
    ) -> None:
        self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            partition=partition,
            on_delivery=lambda err, msg: anyio.from_thread.run(on_delivery, err, msg) if on_delivery else None,
            timestamp=int(timestamp.timestamp()) if isinstance(timestamp, datetime) else timestamp,
            headers=headers,
        )

    async def purge(self, in_queue: bool = True, in_flight: bool = True, blocking: bool = True) -> None:
        await anyio.to_thread.run_sync(self._producer.purge, in_queue, in_flight, blocking)

    async def send_offsets_to_transaction(
        self, positions: Sequence[confluent_kafka.TopicPartition], group_metadata: object, timeout: float | None = None
    ) -> None:
        await anyio.to_thread.run_sync(self._producer.send_offsets_to_transaction, positions, group_metadata, timeout)

    def set_sasl_credentials(self, username: str, password: str) -> None:
        self._producer.set_sasl_credentials(username, password)
