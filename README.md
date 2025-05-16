# async-kafka

[![PyPI - Status](https://img.shields.io/pypi/status/async-kafka)](https://pypi.org/project/async-kafka)
[![PyPI](https://img.shields.io/pypi/v/async-kafka)](https://pypi.org/project/async-kafka)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-kafka)](https://pypi.org/project/async-kafka)
[![PyPI - License](https://img.shields.io/pypi/l/async-kafka)](https://pypi.org/project/async-kafka)
[![CI Status](https://github.com/tkukushkin/async-kafka/actions/workflows/check.yml/badge.svg)](https://github.com/tkukushkin/async-kafka/actions/workflows/check.yml)
[![codecov](https://codecov.io/gh/tkukushkin/async-kafka/graph/badge.svg?token=376OQ1J9YH)](https://codecov.io/gh/tkukushkin/async-kafka)
[![docs](https://readthedocs.org/projects/async-kafka/badge/?version=latest)](https://async-kafka.readthedocs.io/stable/)

Thin async wrapper for [Confluent Kafka Python Client](https://pypi.org/project/confluent-kafka/).

## Installation

```bash
pip install async-kafka
```

## Usage

At the moment, only `Producer`, `Consumer`, and `AdminClient` are supported.

API is similar to the original `confluent-kafka` library, but with async/await syntax.

## License

This project is licensed under the MIT License – see the LICENSE file for details.
