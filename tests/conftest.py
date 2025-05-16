import time
from typing import Any

import confluent_kafka.admin
import pytest
from lovely.pytest.docker.compose import Services


@pytest.fixture(scope='session', autouse=True)
def anyio_backend() -> str:
    return 'asyncio'


@pytest.fixture(scope='session')
def kafka_addr(docker_ip: str) -> str:
    return f'{docker_ip}:9092'


@pytest.fixture(scope='session')
def default_config(kafka_addr) -> dict[str, Any]:
    return {'bootstrap.servers': kafka_addr}


@pytest.fixture(scope='session', autouse=True)
def _start_kafka(kafka_addr, docker_services: Services) -> None:
    docker_services.start('kafka')
    exception = None
    for _ in range(50):
        try:
            confluent_kafka.admin.AdminClient({'bootstrap.servers': kafka_addr}).list_topics()
        except confluent_kafka.KafkaException as exc:
            time.sleep(0.3)
            exception = exc
        else:
            return
    raise TimeoutError from exception
