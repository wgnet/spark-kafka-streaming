package com.wargaming.dwh.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.wargaming.dwh.spark.kafka.{PartitionedSimpleConsumerKafkaInputDStream, KafkaStreamConsumerConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

/**
 * Created by d_balyka on 21.04.14.
 */
object SparkUtils {

  def copyProperty(fromHadoopConf: Configuration, toSparkConf: SparkConf, name: String, defaultValue: String = "") = {
    toSparkConf.set(name, fromHadoopConf.get(name, defaultValue))
  }

  def createStreamPartition(ssc: StreamingContext, partitionConfig: KafkaStreamConsumerConfig, messagesFlatMapper: Array[Byte] => collection.Seq[String] = (a: Array[Byte]) => Array(new String(a, "utf8"))): DStream[(String, String)] = {
    new PartitionedSimpleConsumerKafkaInputDStream(ssc, partitionConfig, messagesFlatMapper)
  }

}
