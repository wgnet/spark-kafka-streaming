package org.apache.spark.examples

import java.util.Properties

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import com.wargaming.dwh.spark.SparkUtils
import com.wargaming.dwh.spark.kafka.KafkaStreamConsumerConfig
import kafka.producer._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming._
import org.apache.spark.{Logging, SparkContext, SparkConf}

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <kafkaZkQuorum>, <group>, <topics>, <numPartitions>, <numThreads>, <hbaseZkQuorum>, <hbaseMonitoringTable>
 * <kafkaZkQuorum> is a list of one or more zookeeper servers that make quorum for kafka
 * <group> is the name of kafka consumer group
 * <topics> is a list of one or more kafka topics to consume from
 * <numPartitions> is the number or partitions of stream
 * <numThreads> is the number of threads the kafka consumer should use
 * <hbaseZkQuorum> is a list of one or more zookeeper servers that make quorum for hbase
 * <hbaseMonitoringTable> is the name of hbase table with stream data
 *
 * Example:
 * `$ java -Dspark.master=spark://spark-server:7077 -cp spark-kafka-streaming-examples-0.1-cdh5.1.0-all.jar
 * org.apache.spark.examples.streaming.KafkaWordCount kafkazoo01,kafkazoo02,kafkazoo03 \
 * my-consumer-group topic1,topic2 3 2 \
 * hbasezoo01,hbasezoo02,hbasezoo03 kafka-stream-monitoring`
 */
object KafkaWordCount extends Logging {
  def main(args: Array[String]) {
    if (args.length < 7) {
      System.err.println("Usage: KafkaWordCount <kafkaZkQuorum>, <group>, <topics>, <numPartitions>, <numThreads>, <hbaseZkQuorum>, <hbaseMonitoringTable>")
      System.exit(1)
    }

    val Array(kafkaZkQuorum, group, topics, numPartitions, numThreads, hbaseZkQuorum, hbaseMonitoringTable) = args

    val sparkConf = new SparkConf()
      .setAppName("KafkaWordCount")

    SparkContext.jarOfClass(this.getClass).map { x =>
      sparkConf.setJars(Seq(x))
    }


    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val lines = ssc.union((1 to numPartitions.toInt).map {
      x =>
        SparkUtils.createStreamPartition(ssc, KafkaStreamConsumerConfig(x,
          numPartitions.toInt,
          numThreads.toInt,
          group,
          kafkaZkQuorum,
          topics.split(","),
          100000,
          1024 * 1024,
          StorageLevel.MEMORY_AND_DISK_SER_2,
          hbaseZkQuorum,
          hbaseMonitoringTable,
          true,
          false)).map(x => x._2)
    })

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messages
    while (true) {
      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        new KeyedMessage[String, String](topic, str)
      }.toArray

      producer.send(messages: _*)
      System.out.println(messages.toList)
      Thread.sleep(100)
    }
  }

}