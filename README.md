spark-kafka-streaming
=====================

Custom Spark Kafka consumer based on Kafka SimpleConsumer API.

Features
- discover kafka metadata from zookeeper (more reliable than from brokers, does not depend on broker list changes)
- reding from multiple topics
- reliably handles leader election and topic reassigment
- saves offsets and stream metadata in hbase (more robust than zookeeper)
- supports metrics via spark metrics mechanism (jmx, graphite, etc.)

Todo
- abstract offset storage
- time controlled offsets commit
- refactor kafka message to rdd elements transformation (flatmapper method)

Usage example in ./examples
