package com.wargaming.dwh.spark.kafka

import org.apache.spark.streaming.dstream._
import kafka.common.ErrorMapping
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{SparkEnv, Logging}
import com.wargaming.dwh.kafka.KafkaUtils
import java.util.{TimerTask, Timer}
import java.util.concurrent.{ExecutorService, Executors}
import kafka.consumer.SimpleConsumer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Get, HTable, Put, Increment}
import java.net.InetAddress
import org.apache.hadoop.hbase.HBaseConfiguration
import scala.collection.mutable
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import com.codahale.metrics.{Gauge, MetricRegistry, Meter}

class ConsumerData(var ts: Long = 0l, var status: Short = 0, var leader: Option[(String, Int)], var consumerId: Option[Int], var offset: Long = 0l) extends Serializable {
  override def toString = {
    "cd" +(ts, status, leader, consumerId, offset)
  }
}

case class KafkaStreamConsumerConfig(var consumerId: Int,
                                     var consumersCount: Int,
                                     var topicReadThreads: Int,
                                     var clientId: String,
                                     var kafkaZkQuorum: String,
                                     var topics: collection.Seq[String],
                                     var soTimeOut: Int = 100000,
                                     var bufferSize: Int,
                                     var storageLevel: StorageLevel,
                                     var zkHbaseQuorum: String,
                                     var offsetsTable: String,
                                     var cleanOffsets: Boolean,
                                     var devMode: Boolean = false) extends Serializable {
  def this() = this(-1, -1, 0, "", "", mutable.Seq[String](), 100000, 1024 * 1024, StorageLevel.MEMORY_AND_DISK_SER_2, "", "", false, false)
}

/**
 * Created by d_balyka on 09.04.14.
 */
class PartitionedSimpleConsumerKafkaInputDStream(@transient ssc_ : StreamingContext,
                                                 config: KafkaStreamConsumerConfig,
                                                 messagesFlatMapper: Array[Byte] => collection.Seq[String] = (a: Array[Byte]) => Array(new String(a, "utf8")))
  extends ReceiverInputDStream[(String, String)](ssc_) with Logging {

  def getReceiver(): Receiver[(String, String)] = {
    new PartitionedSimpleConsumerKafkaReceiver(config, messagesFlatMapper)
      .asInstanceOf[Receiver[(String, String)]]
  }

}

class PartitionedSimpleConsumerKafkaReceiver(config: KafkaStreamConsumerConfig,
                                             messagesFlatMapper: Array[Byte] => collection.Seq[String] = (a: Array[Byte]) => Array(new String(a, "utf8")))
  extends Receiver[Any](config.storageLevel) with Logging {

  class CheckTopicPartitionsTask extends TimerTask() {
    override def run(): Unit = {
      try {
        findTopicPartitions()
      } catch {
        case t: Throwable => log.info("exception during geoip file checker timer task %1$s".format(t), t)
      }
    }
  }

  class Consumer(id: Int) extends Runnable {
    override def run(): Unit = {
      while (true) {
        try {
          val todo = take(id)
          //log.info("taken %1$s by %2$s".format(todo, id))
          if (!todo.isDefined) {
            Thread.sleep(1000)
          } else {
            doFetch(todo.get, id)
            release(todo.get._1)
          }
        }
        catch {
          case t: Throwable => log.error("" + t, t)
        }
      }
    }
  }

  var timer: Option[Timer] = None
  var exe: Option[ExecutorService] = None
  val topicPartitions = collection.mutable.Set[(String, Int)]()

  val tpData = collection.mutable.Map[(String, Int), ConsumerData]()
  val consumersConnectors = (1 to config.topicReadThreads).map(_ -> collection.mutable.Map[(String, Int), SimpleConsumer]()).toMap
  val threadToTable = mutable.Map[String, HTable]()
  val meters = collection.mutable.Map[(String, Int), Meter]()


  def partitionFilter(partition: Int): Boolean = {
    partition % config.consumersCount == config.consumerId
  }

  val processor: (String, String) => Unit = {
    if (config.devMode) {
      (k: String, v: String) =>
      //              log.info("key:%1$s value:%2$s".format(k, v))
    } else {
      (k: String, v: String) =>
        store((k, v))
    }
  }

  def onStop() {
    try {
      timer.map {
        t => t.cancel()
      }
    } catch {
      case t: Throwable => log.warn("error while stop timer scheduler %1$s".format(t), t)
    }

    try {
      exe.map {
        e => e.shutdownNow()
      }
    } catch {
      case t: Throwable => log.warn("error while stop executors pool %1$s".format(t), t)
    }

  }

  def onStart() {

    log.info("starting")

    timer = Some(new Timer())
    timer.get.schedule(new CheckTopicPartitionsTask(), 500, 15000)

    exe = Some(Executors.newFixedThreadPool(config.topicReadThreads))

    (1 to config.topicReadThreads).foreach {
      x =>
        exe.get.execute(new Consumer(x))
    }

  }

  def readOffset(tp: (String, Int)): Long = {
    var result = 0l
    try {
      val table = getHbaseTable(Thread.currentThread().getName)
      val rk = Bytes.toBytes("[%3$s]-%1$s-%2$s".format(tp._1, tp._2, config.clientId))
      val get = new Get(rk)
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("o"))
      val res = table.get(get)
      if (!res.isEmpty) {
        result = Bytes.toString(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("o"))).toLong
      }
    } catch {
      case e: Exception => {
        log.error("" + e, e)
      }
    }
    result
  }

  def addNewTpData(tps: collection.Seq[(String, Int)]) {

    val metricRegistry = SparkEnv.get.metricsSystem.registry

    tpData.synchronized {
      tps.foreach {
        x =>
          if (!tpData.contains(x)) {
            val consumer = new ConsumerData(0, ErrorMapping.NotLeaderForPartitionCode, None, None)
            if (!config.cleanOffsets) {
              consumer.offset = readOffset(x)
            }
            tpData += (x -> consumer)

            metricRegistry.register(MetricRegistry.name(config.clientId, "offset", "%1$s.%2$s".format(x._1, x._2)), new Gauge[Long] {
              override def getValue(): Long = {
                tpData.get(x).map {
                  tpd => tpd.offset
                } getOrElse (0l)
              }
            })
            metricRegistry.register(MetricRegistry.name(config.clientId, "status", "%1$s.%2$s".format(x._1, x._2)), new Gauge[Short] {
              override def getValue(): Short = {
                tpData.get(x).map {
                  tpd => tpd.status
                } getOrElse (ErrorMapping.NoError)
              }
            })
            meters.put(x, metricRegistry.meter(MetricRegistry.name(config.clientId, "consumptionRate", "%1$s.%2$s".format(x._1, x._2))))
          }
      }
    }
  }


  def take(consumerId: Int): Option[((String, Int), ConsumerData)] = {
    var result: Option[((String, Int), ConsumerData)] = None
    tpData.synchronized {
      //find
      if (!tpData.isEmpty) {
        val candidate = tpData.minBy {
          x => if (x._2.consumerId.isDefined) {
            Long.MaxValue
          } else {
            x._2.ts
          }
        }
        if (!candidate._2.consumerId.isDefined) {
          candidate._2.consumerId = Some(consumerId)
          result = Some(candidate)
        }
      }
    }
    result
  }

  def release(tp: (String, Int)) = {
    tpData.synchronized {
      tpData.get(tp) map {
        x =>
          x.consumerId = None
          x.ts = System.currentTimeMillis()
      }
    }
  }

  def findTopicPartitions() = {
    val tp = config.topics.flatMap {
      topic =>
        KafkaUtils.findPartitionsWithLeadersFromZk(config.kafkaZkQuorum, topic).filter {
          x => partitionFilter(x._1)
        }.toList.map {
          x => (topic, x._1)
        }
    }.toSet

    val newTp = tp.diff(topicPartitions)

    addNewTpData(newTp.toSeq)

    topicPartitions ++= newTp

    log.info("" + tpData)
    log.info("" + consumersConnectors)

  }

  def doFetch(tp: ((String, Int), ConsumerData), consumerId: Int) = {
    fixErrorStates(tp: ((String, Int), ConsumerData), consumerId: Int)
    val leader = getLeader(tp, consumerId)
    var count = 0l
    leader._1.map {
      l =>
        val data = KafkaUtils.getDataFromLeader(l, tp._1._1, tp._1._2, tp._2.offset, config.bufferSize, config.clientId)

        val status = data._1
        tp._2.status = status

        if (ErrorMapping.NoError != status) {
          log.error("error for topic %1$s partition %2$s = %3$s".format(tp._1._1, tp._1._2, status))
        } else {
          if (data._2.isDefined) {
            data._2.get.foreach {
              x =>
                tp._2.offset = x._2
                val key: String = if (x._3.isDefined) {
                  new String(x._3.get, "utf8")
                } else {
                  null
                }
                messagesFlatMapper(x._4).foreach(m => processor(key, m))
                count += 1
            }
            //            log.info("received %1$s messages for %2$s".format(count, tp._1))
          }
        }
    } getOrElse {
      tp._2.status = leader._2
    }

    persistOffset(tp, count)

  }

  def fixErrorStates(tp: ((String, Int), ConsumerData), consumerId: Int) = {
    try {
      //analyze last status
      tp._2.status match {
        case ErrorMapping.NoError => {
          //do nothing
        }
        case ErrorMapping.LeaderNotAvailableCode => {
          invalidateConsumer(tp, consumerId)
        }
        case ErrorMapping.NotLeaderForPartitionCode => {
          tp._2.leader = None
        }
        case ErrorMapping.OffsetOutOfRangeCode => {
          fixOffsets(tp, consumerId)
        }
        case s: Short => {
          log.error("unsuported status %1$s!".format(s))
          s
        }
      }
    } catch {
      case e: Exception => {
        log.error("" + e, e)
      }
    }
  }

  def fixOffsets(tp: ((String, Int), ConsumerData), consumerId: Int) = {
    try {
      val currentOffset = tp._2.offset
      getLeader(tp, consumerId)._1.map {
        sc =>
          val newOffsetData = KafkaUtils.findOffsetsForPartitionWithConnection(sc, tp._1._1, tp._1._2, config.clientId)
          val newOffset = newOffsetData._2.map {
            seq => seq.find(x => x > currentOffset).getOrElse(tp._2.offset)
          } getOrElse (tp._2.offset)
          log.info("new offset for %1$s = %2$s".format(tp._1, newOffset))
          tp._2.offset = newOffset
          tp._2.status = newOffsetData._1
      }
    } catch {
      case e: Exception => {
        log.error("" + e, e)
      }
    }
  }

  def getHbaseTable(threadId: String): HTable = {
    var table = threadToTable.get(threadId).getOrElse(null)
    if (table == null) {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", config.zkHbaseQuorum)
      table = new HTable(conf, config.offsetsTable)
      threadToTable.put(threadId, table)
    }
    table
  }


  def persistOffset(tp: ((String, Int), ConsumerData), consumed: Long = 0l) {
    try {

      meters.get(tp._1) map {
        m => m.mark(consumed)
      }

      val table = getHbaseTable(Thread.currentThread().getName)

      val rk = Bytes.toBytes("[%3$s]-%1$s-%2$s".format(tp._1._1, tp._1._2, config.clientId))

      val inc = new Increment(rk)
      inc.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c"), consumed)
      val incRes = table.increment(inc)

      val put = new Put(rk)
      put.add(Bytes.toBytes("d"), Bytes.toBytes("o"), Bytes.toBytes("" + tp._2.offset))
      put.add(Bytes.toBytes("d"), Bytes.toBytes("p"), Bytes.toBytes("" + Bytes.toLong(incRes.getValue(Bytes.toBytes("d"), Bytes.toBytes("c")))))
      put.add(Bytes.toBytes("d"), Bytes.toBytes("h"), Bytes.toBytes("" + InetAddress.getLocalHost().getHostName()))
      put.add(Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("" + tp._2.status))
      put.add(Bytes.toBytes("d"), Bytes.toBytes("l"), Bytes.toBytes("" + tp._2.leader))
      put.add(Bytes.toBytes("d"), Bytes.toBytes("t"), Bytes.toBytes("" + tp._1._1))
      put.add(Bytes.toBytes("d"), Bytes.toBytes("tp"), Bytes.toBytes("" + tp._1._2))

      table.put(put)
      table.flushCommits()
    } catch {
      case e: Exception => {
        log.error("" + e, e)
      }
    }
  }


  def getLeader(tp: ((String, Int), ConsumerData), consumerId: Int): (Option[SimpleConsumer], Short) = {
    var result: Option[SimpleConsumer] = None
    val status = ErrorMapping.NoError
    val mapData = consumersConnectors.get(consumerId).get
    if (!tp._2.leader.isDefined) {
      //get new leader info
      val newLeader = selectNewLeader(tp)
      tp._2.leader = newLeader
      tp._2.leader.map {
        x =>
          //start new leader
          result = getOrCreateConsumer(mapData, x)
      }
    } else {
      result = getOrCreateConsumer(mapData, tp._2.leader.get)
    }
    (result, status)
  }

  def invalidateConsumer(tp: ((String, Int), ConsumerData), consumerId: Int) = {
    val mapData = consumersConnectors.get(consumerId).get
    tp._2.leader.map {
      x =>
        mapData.get(x).map {
          y =>
            //close old leader
            safeCloseConsumer(y)
        }
        //remove old leader
        mapData.remove(x)
    }
    tp._2.leader = None
  }

  def safeCloseConsumer(consumer: SimpleConsumer) = {
    try {
      consumer.close()
    } catch {
      case t: Throwable => log.warn("error while closing consumer %1$s".format(t), t)
    }
  }

  def getOrCreateConsumer(map: collection.mutable.Map[(String, Int), SimpleConsumer], key: (String, Int)): Option[SimpleConsumer] = {
    var result: Option[SimpleConsumer] = None
    map.get(key).map {
      x =>
        result = Some(x)
    }.getOrElse {
      val consumer = new SimpleConsumer(key._1, key._2, config.soTimeOut, config.bufferSize, config.clientId)
      map.put(key, consumer)
      result = Some(consumer)
    }
    result
  }


  def selectNewLeader(tp: ((String, Int), ConsumerData)): Option[(String, Int)] = {
    var result: Option[(String, Int)] = None
    try {
      result = KafkaUtils.findLeadersForPartitionFromZk(config.kafkaZkQuorum, tp._1._1, Array(tp._1._2)) get (tp._1._2)
    } catch {
      case e: Exception => {
        log.error("" + e, e)
      }
    }
    result
  }


}

