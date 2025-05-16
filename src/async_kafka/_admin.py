from collections.abc import Coroutine, Mapping, Sequence, Set as AbstractSet
from typing import Any, Literal, overload

import anyio
import anyio.to_thread
import confluent_kafka
import confluent_kafka.admin

from ._utils import FuturesDict, wrap_concurrent_future


class AdminClient:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._admin_client = confluent_kafka.admin.AdminClient(dict(config))

    def create_topics(
        self,
        new_topics: Sequence[confluent_kafka.admin.NewTopic],
        *,
        operation_timeout: float | None = None,
        request_timeout: float | None = None,
        validate_only: bool = False,
    ) -> FuturesDict[str, None]:
        kwargs: dict[str, Any] = {}
        if operation_timeout is not None:
            kwargs['operation_timeout'] = operation_timeout
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        if validate_only:
            kwargs['validate_only'] = validate_only
        return FuturesDict.from_concurrent_futures(self._admin_client.create_topics(new_topics, **kwargs))

    def delete_topics(
        self,
        topics: Sequence[str],
        *,
        operation_timeout: float | None = None,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, None]:
        kwargs: dict[str, Any] = {}
        if operation_timeout is not None:
            kwargs['operation_timeout'] = operation_timeout
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.delete_topics(topics, **kwargs))

    async def list_topics(
        self, topic: str | None = None, timeout: float | None = None
    ) -> confluent_kafka.admin.ClusterMetadata:
        if timeout is None:
            timeout = -1
        return await anyio.to_thread.run_sync(self._admin_client.list_topics, topic, timeout)

    def create_partitions(
        self,
        new_partitions: Sequence[confluent_kafka.admin.NewPartitions],
        *,
        operation_timeout: float | None = None,
        request_timeout: float | None = None,
        validate_only: bool = False,
    ) -> FuturesDict[str, None]:
        kwargs: dict[str, Any] = {}
        if operation_timeout is not None:
            kwargs['operation_timeout'] = operation_timeout
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        if validate_only:
            kwargs['validate_only'] = validate_only
        return FuturesDict.from_concurrent_futures(self._admin_client.create_partitions(new_partitions, **kwargs))

    def describe_configs(
        self,
        resources: Sequence[confluent_kafka.admin.ConfigResource],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.admin.ConfigResource, dict[str, confluent_kafka.admin.ConfigEntry]]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.describe_configs(resources, **kwargs))

    def incremental_alter_configs(
        self,
        resources: Sequence[confluent_kafka.admin.ConfigResource],
        *,
        request_timeout: float | None = None,
        validate_only: bool = False,
        broker: int | None = None,
    ) -> FuturesDict[confluent_kafka.admin.ConfigResource, None]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        if validate_only:
            kwargs['validate_only'] = validate_only
        if broker is not None:
            kwargs['broker'] = broker
        return FuturesDict.from_concurrent_futures(self._admin_client.incremental_alter_configs(resources, **kwargs))

    def create_acls(
        self,
        acls: Sequence[confluent_kafka.admin.AclBinding],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.admin.AclBinding, None]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.create_acls(acls, **kwargs))

    async def describe_acls(
        self,
        acl_binding_filter: confluent_kafka.admin.AclBindingFilter,
        *,
        request_timeout: float | None = None,
    ) -> list[confluent_kafka.admin.AclBinding]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return await wrap_concurrent_future(self._admin_client.describe_acls(acl_binding_filter, **kwargs))

    def delete_acls(
        self,
        acl_binding_filters: Sequence[confluent_kafka.admin.AclBindingFilter],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.admin.AclBindingFilter, None]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.delete_acls(acl_binding_filters, **kwargs))

    async def list_consumer_groups(
        self,
        *,
        request_timeout: float | None = None,
        states: AbstractSet[confluent_kafka.ConsumerGroupState] | None = None,
        types: AbstractSet[confluent_kafka.ConsumerGroupType] | None = None,
    ) -> confluent_kafka.admin.ListConsumerGroupsResult:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        if states is not None:
            kwargs['states'] = states
        if types is not None:
            kwargs['types'] = types
        return await wrap_concurrent_future(self._admin_client.list_consumer_groups(**kwargs))

    def describe_consumer_groups(
        self,
        group_ids: Sequence[str],
        *,
        include_authorized_operations: bool = False,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, confluent_kafka.admin.ConsumerGroupDescription]:
        kwargs: dict[str, Any] = {}
        if include_authorized_operations:
            kwargs['include_authorized_operations'] = include_authorized_operations
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.describe_consumer_groups(group_ids, **kwargs))

    def describe_topics(
        self,
        topics: Sequence[str],
        *,
        include_authorized_operations: bool = False,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, confluent_kafka.admin.TopicDescription]:
        kwargs: dict[str, Any] = {}
        if include_authorized_operations:
            kwargs['include_authorized_operations'] = include_authorized_operations
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.describe_topics(topics, **kwargs))

    async def describe_cluster(
        self,
        *,
        include_authorized_operations: bool = False,
        request_timeout: float | None = None,
    ) -> confluent_kafka.admin.DescribeClusterResult:
        kwargs: dict[str, Any] = {}
        if include_authorized_operations:
            kwargs['include_authorized_operations'] = include_authorized_operations
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return await wrap_concurrent_future(self._admin_client.describe_cluster(**kwargs))

    def delete_consumer_groups(
        self, group_ids: Sequence[str], *, request_timeout: float | None = None
    ) -> FuturesDict[str, None]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.delete_consumer_groups(group_ids, **kwargs))

    def list_consumer_group_offsets(
        self,
        list_consumer_group_offsets_request: Sequence[confluent_kafka.ConsumerGroupTopicPartitions],
        *,
        require_stable: bool = False,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, confluent_kafka.ConsumerGroupTopicPartitions]:
        kwargs: dict[str, Any] = {}
        if require_stable:
            kwargs['require_stable'] = require_stable
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(
            self._admin_client.list_consumer_group_offsets(list_consumer_group_offsets_request, **kwargs)
        )

    def alter_consumer_group_offsets(
        self,
        alter_consumer_group_offsets_request: Sequence[confluent_kafka.ConsumerGroupTopicPartitions],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, confluent_kafka.ConsumerGroupTopicPartitions]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(
            self._admin_client.alter_consumer_group_offsets(alter_consumer_group_offsets_request, **kwargs)
        )

    def set_sasl_credentials(self, username: str, password: str) -> None:
        self._admin_client.set_sasl_credentials(username, password)

    @overload
    async def describe_user_scram_credentials(
        self, users: Literal[None] = None, *, request_timeout: float | None = None
    ) -> dict[str, confluent_kafka.admin.UserScramCredentialsDescription]: ...

    @overload
    def describe_user_scram_credentials(
        self, users: Sequence[str], *, request_timeout: float | None = None
    ) -> FuturesDict[str, confluent_kafka.admin.UserScramCredentialsDescription]: ...

    @overload
    def describe_user_scram_credentials(
        self, users: None | Sequence[str], *, request_timeout: float | None = None
    ) -> (
        Coroutine[Any, Any, dict[str, confluent_kafka.admin.UserScramCredentialsDescription]]
        | FuturesDict[str, confluent_kafka.admin.UserScramCredentialsDescription]
    ): ...

    def describe_user_scram_credentials(
        self,
        users: Sequence[str] | None = None,
        *,
        request_timeout: float | None = None,
    ) -> (
        Coroutine[Any, Any, dict[str, confluent_kafka.admin.UserScramCredentialsDescription]]
        | FuturesDict[str, confluent_kafka.admin.UserScramCredentialsDescription]
    ):
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        if users is None:
            return wrap_concurrent_future(self._admin_client.describe_user_scram_credentials(**kwargs))  # pyright: ignore [reportArgumentType]
        return FuturesDict.from_concurrent_futures(self._admin_client.describe_user_scram_credentials(users, **kwargs))  # pyright: ignore [reportArgumentType]

    def alter_user_scram_credentials(
        self,
        alterations: Sequence[confluent_kafka.admin.UserScramCredentialAlteration],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, None]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(
            self._admin_client.alter_user_scram_credentials(alterations, **kwargs)
        )

    def list_offsets(
        self,
        topic_partition_offsets: Mapping[confluent_kafka.TopicPartition, confluent_kafka.admin.OffsetSpec],
        *,
        isolation_level: confluent_kafka.IsolationLevel | None = None,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.TopicPartition, confluent_kafka.admin.ListOffsetsResultInfo]:
        kwargs: dict[str, Any] = {}
        if isolation_level is not None:
            kwargs['isolation_level'] = isolation_level
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.list_offsets(topic_partition_offsets, **kwargs))

    def delete_records(
        self,
        topic_partition_offsets: Sequence[confluent_kafka.TopicPartition],
        *,
        request_timeout: float | None = None,
        operation_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.TopicPartition, confluent_kafka.admin.DeletedRecords]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        if operation_timeout is not None:
            kwargs['operation_timeout'] = operation_timeout
        return FuturesDict.from_concurrent_futures(self._admin_client.delete_records(topic_partition_offsets, **kwargs))

    async def elect_leaders(
        self,
        election_type: confluent_kafka.ElectionType,
        partitions: Sequence[confluent_kafka.TopicPartition] | None = None,
        *,
        request_timeout: float | None = None,
        operation_timeout: float | None = None,
    ) -> dict[confluent_kafka.TopicPartition, confluent_kafka.KafkaError | None]:
        kwargs: dict[str, Any] = {}
        if request_timeout is not None:
            kwargs['request_timeout'] = request_timeout
        if operation_timeout is not None:
            kwargs['operation_timeout'] = operation_timeout
        return await anyio.to_thread.run_sync(  # pyright: ignore [reportReturnType]
            lambda: self._admin_client.elect_leaders(election_type, partitions, **kwargs)
        )
