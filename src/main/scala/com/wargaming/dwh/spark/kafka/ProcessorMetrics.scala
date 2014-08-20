package com.wargaming.dwh.spark.kafka

import com.wargaming.dwh.kafka.KafkaUtils
import kafka.consumer.SimpleConsumer
import kafka.common.TopicAndPartition
import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{HTable, Get}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import scala.collection.JavaConversions._
import scala.collection.parallel.immutable
import org.apache.spark.Logging

/**
 * Created by d_balyka on 05.06.14.
 */
object ProcessorMetrics extends Logging {


  def calculateDeltas(kafkaZk: String, topic: String, group: String, tableName: String, hbaseConf: String): collection.Map[Int, (Long, Long)] = {

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path(hbaseConf))
    val table = new HTable(conf, tableName)

    val result = collection.mutable.Map[Int, (Long, Long)]()

    try {

      val partitionLeaders = KafkaUtils.findPartitionsWithLeadersFromZk(kafkaZk, topic)


      val kafkaOffsets = partitionLeaders.groupBy {
        x => x._2
      }.flatMap {
        x =>

          val consumer = new SimpleConsumer(x._1._1, x._1._2, 10000, 100 * 1024, "CheckMetrics")

          val tps = x._2.map {
            p =>
              (p._1, TopicAndPartition(topic, p._1))
          }

          val request = OffsetRequest(tps.map {
            p => p._2 -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
          }.asInstanceOf[collection.immutable.Map[TopicAndPartition, PartitionOffsetRequestInfo]])

          var partitionOffset: collection.Map[Int, Long] = Map[Int, Long]()

          try {
            val kafkaOffset = consumer.getOffsetsBefore(request)
            partitionOffset = kafkaOffset.partitionErrorAndOffsets.map {
              po => (po._1.partition, po._2.offsets.head)
            }

          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            consumer.close()
          }

          partitionOffset

      }

      val hbaseGets = partitionLeaders.map {
        p =>
          val rk = Bytes.toBytes("[%3$s]-%1$s-%2$s".format(topic, p._1, group))

          val get = new Get(rk)
          get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("o"))
      }

      val hbaseOffsets = table.get(hbaseGets.toBuffer).map {
        r =>
          if (r.getRow != null) {
            val partition = Bytes.toString(r.getRow).split("-").reverse.head.toInt
            (partition, Bytes.toString(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("o"))).toLong)
          } else {
            (-1, -1l)
          }
      }.toMap

      partitionLeaders.foreach {
        p =>
          result.put(p._1, (kafkaOffsets.getOrElse(p._1, -1l), hbaseOffsets.getOrElse(p._1, -1l)))
      }

    } catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      table.close()
    }


    result

  }


}
