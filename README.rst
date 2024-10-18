aiosafeconsumer
===============

aiosafeconsumer is a library that provides abstractions and some implementations
to consume data somewhere and process it.

Abstractions:

* `DataSource` - waits for data and returns batch of records using Python generator
* `DataProcessor` - accepts batch of records and precess it
* `DataTransformer` - accepts batch of records and transform it and calls
  another processor to precess it. Extends `DataProcessor`
* `Worker` - abstract worker
* `ConsumerWorker` - connects `DataSource` and `DataProcessor`
* `DataWriter` - base abstraction to perform data synchronization

Current implementations:

* `KafkaSource` - read data from Kafka
* `RedisWriter` - synchronize data in Redis
* `WorkerPool` - controller to setup and run workers in parallel. Can handle worker failures and restarts workers when it fails or exits.

Recommend producer library: https://github.com/lostclus/django-kafka-streamer

Example application: https://github.com/lostclus/WeatherApp
