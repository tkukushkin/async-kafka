import sys
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from types import TracebackType
from typing import Any, Literal, overload

import anyio
import anyio.from_thread
import anyio.to_thread
import confluent_kafka
import confluent_kafka.admin

from ._utils import async_to_sync

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


_AsyncAsyncCallback = Callable[
    [confluent_kafka.Consumer, Sequence[confluent_kafka.TopicPartition]], Coroutine[Any, Any, Any]
]


class Consumer:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._consumer = confluent_kafka.Consumer(dict(config))

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        with anyio.CancelScope(shield=True):
            await self.close()

    def assign(self, partitions: Sequence[confluent_kafka.TopicPartition]) -> None:
        self._consumer.assign(partitions)

    def assignment(self) -> list[confluent_kafka.TopicPartition]:
        return self._consumer.assignment()

    async def close(self) -> None:
        await anyio.to_thread.run_sync(self._consumer.close)

    @overload
    def commit(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Sequence[confluent_kafka.TopicPartition] | None = None,
        *,
        asynchronous: Literal[True] = True,
    ) -> None: ...

    @overload
    async def commit(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Sequence[confluent_kafka.TopicPartition] | None = None,
        *,
        asynchronous: Literal[False],
    ) -> list[confluent_kafka.TopicPartition]: ...

    def commit(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Sequence[confluent_kafka.TopicPartition] | None = None,
        *,
        asynchronous: bool = True,
    ) -> None | Coroutine[Any, Any, list[confluent_kafka.TopicPartition]]:
        if asynchronous:
            self._consumer.commit(message, offsets)
        return anyio.to_thread.run_sync(self._consumer.commit, message, offsets, False)

    async def committed(
        self, partitions: Sequence[confluent_kafka.TopicPartition], timeout: float | None = None
    ) -> list[confluent_kafka.TopicPartition]:
        return await anyio.to_thread.run_sync(self._consumer.committed, partitions, timeout)

    async def consume(self, num_messages: int = 1, timeout: float | None = None) -> list[confluent_kafka.Message]:
        if timeout is None:
            timeout = -1
        return await anyio.to_thread.run_sync(self._consumer.consume, num_messages, timeout)

    def consumer_group_metadata(self) -> object:
        return self._consumer.consumer_group_metadata()

    @overload
    def get_watermark_offsets(
        self, partition: confluent_kafka.TopicPartition, *, cached: Literal[True]
    ) -> tuple[int, int]: ...

    @overload
    async def get_watermark_offsets(
        self, partition: confluent_kafka.TopicPartition, timeout: float | None = None, *, cached: Literal[False] = False
    ) -> tuple[int, int]: ...

    def get_watermark_offsets(
        self,
        partition: confluent_kafka.TopicPartition,
        timeout: float | None = None,
        *,
        cached: bool = False,
    ) -> tuple[int, int] | Coroutine[Any, Any, tuple[int, int]]:
        if cached:
            return self._consumer.get_watermark_offsets(partition)
        return anyio.to_thread.run_sync(self._consumer.get_watermark_offsets, partition, timeout, False)

    def incremental_assign(self, partitions: Sequence[confluent_kafka.TopicPartition]) -> None:
        self._consumer.incremental_assign(partitions)

    def incremental_unassign(self, partitions: Sequence[confluent_kafka.TopicPartition]) -> None:
        self._consumer.incremental_unassign(partitions)

    async def list_topics(
        self, topic: str | None = None, timeout: float | None = None
    ) -> confluent_kafka.admin.ClusterMetadata:
        if timeout is None:
            timeout = -1
        return await anyio.to_thread.run_sync(self._consumer.list_topics, topic, timeout)

    def memberid(self) -> str | None:
        return self._consumer.memberid()

    async def offsets_for_times(
        self, partitions: Sequence[confluent_kafka.TopicPartition], timeout: float | None = None
    ) -> list[confluent_kafka.TopicPartition]:
        return await anyio.to_thread.run_sync(self._consumer.offsets_for_times, partitions, timeout)

    def pause(self, partitions: Sequence[confluent_kafka.TopicPartition]) -> None:
        self._consumer.pause(partitions)

    async def poll(self, timeout: float | None = None) -> confluent_kafka.Message | None:
        return await anyio.to_thread.run_sync(self._consumer.poll, timeout)

    def position(
        self, partitions: Sequence[confluent_kafka.TopicPartition] | None = None
    ) -> list[confluent_kafka.TopicPartition]:
        return self._consumer.position(partitions) if partitions else self._consumer.position()

    def resume(self, partitions: Sequence[confluent_kafka.TopicPartition]) -> None:
        self._consumer.resume(partitions)

    def seek(self, partition: confluent_kafka.TopicPartition) -> None:
        self._consumer.seek(partition)

    def set_sasl_credentials(self, username: str, password: str) -> None:
        self._consumer.set_sasl_credentials(username, password)

    def store_offsets(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Sequence[confluent_kafka.TopicPartition] | None = None,
    ) -> None:
        self._consumer.store_offsets(message, offsets)

    def subscribe(
        self,
        topics: Iterable[str],
        *,
        on_assign: (_AsyncAsyncCallback | None) = None,
        on_revoke: (_AsyncAsyncCallback | None) = None,
        on_lost: (_AsyncAsyncCallback | None) = None,
    ) -> None:
        self._consumer.subscribe(
            list(topics),
            on_assign=async_to_sync(on_assign) if on_assign else None,
            on_revoke=async_to_sync(on_revoke) if on_revoke else None,
            on_lost=async_to_sync(on_lost) if on_lost else None,
        )

    def unassign(self) -> None:
        self._consumer.unassign()

    def unsubscribe(self) -> None:
        self._consumer.unsubscribe()
