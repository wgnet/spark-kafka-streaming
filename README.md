spark-kafka-streaming
=====================

Custom Spark Kafka consumer based on Kafka SimpleConsumer API.

Features
- discover kafka metadata from zookeeper (more reliable than from brokers, does not depend on broker list changes)
- multiple topics
- partitions adding for topics
- partition leaders changing
- partition w/o leader (waits in background for leader)
- stream partitioning
- saves offsets and stream metadata in hbase (more reliable than zookeeper)
- supports metrics via spark metrics mechanism (jmx, graphit, etc.)



Usage examples in ./examples


