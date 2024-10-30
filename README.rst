aiosafeconsumer
===============

.. image:: https://github.com/lostclus/aiosafeconsumer/actions/workflows/tests.yml/badge.svg
    :target: https://github.com/lostclus/aiosafeconsumer/actions

.. image:: https://img.shields.io/pypi/v/aiosafeconsumer.svg
    :target: https://pypi.org/project/aiosafeconsumer/
    :alt: Current version on PyPi

.. image:: https://img.shields.io/pypi/pyversions/aiosafeconsumer
    :alt: PyPI - Python Version

aiosafeconsumer is a library that provides abstractions and some implementations
to consume data somewhere and process it.

Features:

* Based on AsyncIO
* Type annotated
* Use logging with contextual information

Abstractions:

* `DataSource` - waits for data and returns batch of records using Python generator
* `DataProcessor` - accepts batch of records and precess it
* `DataTransformer` - accepts batch of records and transform it and calls
  another processor to precess it. Extends `DataProcessor`
* `Worker` - abstract worker. Do a long running task
* `ConsumerWorker` - connects `DataSource` and `DataProcessor`. Extends Worker
* `DataWriter` - base abstraction to perform data synchronization. Extends DataProcessor

Current implementations:

* `KafkaSource` - read data from Kafka
* `RedisWriter` - synchronize data in Redis
* `ElasticsearchWriter` - synchronize data in Elasticsearch
* `WorkerPool` - controller to setup and run workers in parallel. Can handle worker failures and restarts workers when it fails or exits.

Install::

    pip install aiosafeconsumer

Install with Kafka::

    pip install aiosafeconsumer[kafka]

Install with Redis::

    pip install aiosafeconsumer[redis]

Install with Elasticsearch::

    pip install aiosafeconsumer[elasticsearch]

Links:

* Producer library: https://github.com/lostclus/django-kafka-streamer
* Example application: https://github.com/lostclus/WeatherApp

TODO:

* MongoDB writer
* PostgreSQL writer
* ClickHouse writer
