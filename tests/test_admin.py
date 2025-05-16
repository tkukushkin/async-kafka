from collections.abc import Awaitable
from uuid import uuid1

import confluent_kafka.admin
import pytest

import async_kafka


@pytest.fixture(scope='module')
async def admin_client(default_config) -> async_kafka.AdminClient:
    return async_kafka.AdminClient(default_config)


async def test_create_topics(admin_client):
    name = str(uuid1())
    result = await admin_client.create_topics([confluent_kafka.admin.NewTopic(name)])
    assert result == {name: None}


async def test_list_topics(admin_client):
    metadata = await admin_client.list_topics()
    assert isinstance(metadata, confluent_kafka.admin.ClusterMetadata)


async def test_describe_user_scram_credentials__no_users(admin_client):
    with pytest.raises(confluent_kafka.KafkaException):
        await admin_client.describe_user_scram_credentials()


async def test_describe_user_scram_credentials__with_users_and_await_one(admin_client):
    result = admin_client.describe_user_scram_credentials(users=['user1'])
    assert list(result.keys()) == ['user1']
    assert isinstance(result['user1'], Awaitable)
    with pytest.raises(confluent_kafka.KafkaException):
        await result['user1']


async def test_describe_user_scram_credentials__with_users_and_await_all(admin_client):
    with pytest.raises(confluent_kafka.KafkaException):
        await admin_client.describe_user_scram_credentials(users=['user1'])
