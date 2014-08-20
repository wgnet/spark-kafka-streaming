package com.wargaming.dwh.kafka

import kafka.consumer.SimpleConsumer
import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import java.util.Random
import scala.collection.mutable
import kafka.common.{ErrorMapping, TopicAndPartition}
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.slf4j.LoggerFactory

/**
 * Created by d_balyka on 17.04.14.
 */
object KafkaUtils {

  val log = LoggerFactory.getLogger(KafkaUtils.getClass)

  def findOffsetsForPartition(partitionLeaders: collection.Map[Int, (String, Int)], topic: String, clientId: String = "", time: Long = OffsetRequest.LatestTime, soTimeout: Int = 5000, bufferSize: Int = 100 * 1024): collection.Map[Int, collection.Seq[Long]] = {
    partitionLeaders.map(x => {
      val connection: SimpleConsumer = new SimpleConsumer(x._2._1, x._2._2, soTimeout, bufferSize, clientId)
      val request = new OffsetRequest(Map(new TopicAndPartition(topic, x._1) -> new PartitionOffsetRequestInfo(time, Int.MaxValue)), OffsetRequest.CurrentVersion)
      var result: Option[(Int, collection.Seq[Long])] = None
      try {


        println("%1$s-%2$s  ".format(topic, x._1) + connection.earliestOrLatestOffset(new TopicAndPartition(topic, x._1), OffsetRequest.LatestTime, 111))
        println("%1$s-%2$s  ".format(topic, x._1) + connection.earliestOrLatestOffset(new TopicAndPartition(topic, x._1), OffsetRequest.EarliestTime, 111))

        val response = connection.getOffsetsBefore(request)

        response.partitionErrorAndOffsets.foreach(p => {
          if (p._2.error == ErrorMapping.NoError) {
            result = Some((x._1, p._2.offsets.sortBy[Long](x => x).toSeq))
          }
        })

      } catch {
        case e: Exception => log.info("error while getting leader from broker [%2$s] for topic [%3$s] and partition [%4$s] = %1$s".format(e, x._2, topic, x._1), e)
      } finally {
        try {
          connection.close()
        } catch {
          case e: Exception => log.info("error while closing SimpleConsumer %1$s".format(e), e)
        }
      }
      result.getOrElse((x._1, Seq[Long]()))
    })
  }

  def findOffsetsForPartitionWithConnection(connection: SimpleConsumer, topic: String, partition: Int, clientId: String = "", time: Long = OffsetRequest.LatestTime, soTimeout: Int = 5000, bufferSize: Int = 100 * 1024): (Short, Option[collection.Seq[Long]]) = {
    val request = new OffsetRequest(Map(new TopicAndPartition(topic, partition) -> new PartitionOffsetRequestInfo(time, Int.MaxValue)), OffsetRequest.CurrentVersion)
    var result: (Short, Option[collection.Seq[Long]]) = (ErrorMapping.NoError, None)
    try {
      val response = connection.getOffsetsBefore(request)
      response.partitionErrorAndOffsets.foreach {
        p =>
          if (p._2.error == ErrorMapping.NoError) {
            result = (p._2.error, Some(p._2.offsets.sorted.toSeq))
          } else {
            result = (p._2.error, None)
          }
      }
    } catch {
      case e: Exception => log.info("error while getting offsets from broker [%2$s] for topic [%3$s] and partition [%4$s] = %1$s".format(e, connection, topic, partition), e)
    }
    result
  }


  def findLeadersForPartitionFromZk(zkQuorum: String, topic: String, partitions: Seq[Int], sessionTimeout: Int = 10000, connectionTimeout: Int = 10000): collection.Map[Int, (String, Int)] = {
    val result = mutable.Map[Int, (String, Int)]()
    val zkClient = new ZkClient(zkQuorum, sessionTimeout, connectionTimeout, ZKStringSerializer)
    try {

      val zkBrokersResult = ZkUtils.getAllBrokersInCluster(zkClient).map(b => (b.id, (b.host, b.port))).toMap

      val zkLeadersResult = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, partitions.map(x => new TopicAndPartition(topic, x)).toSet)
      zkLeadersResult.foreach(x => {
        result.put(x._1.partition, zkBrokersResult(x._2.leaderAndIsr.leader))
      })
    } catch {
      case e: Exception => {
        log.info("error while findLeadersForPartitionFromZk %1$s".format(e), e)
        throw e
      }
    }
    finally {
      zkClient.close()
    }
    result
  }

  def findPartitionsWithLeadersFromZk(zkQuorum: String, topic: String, sessionTimeout: Int = 10000, connectionTimeout: Int = 10000): collection.Map[Int, (String, Int)] = {
    val result = mutable.Map[Int, (String, Int)]()
    val zkClient = new ZkClient(zkQuorum, sessionTimeout, connectionTimeout, ZKStringSerializer)
    try {

      val zkBrokersResult = ZkUtils.getAllBrokersInCluster(zkClient).map(b => (b.id, (b.host, b.port))).toMap

      val zkLeadersResult = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, ZkUtils.getPartitionsForTopics(zkClient, Array(topic))(topic).map(x => new TopicAndPartition(topic, x)).toSet)
      zkLeadersResult.foreach(x => {
        result.put(x._1.partition, zkBrokersResult(x._2.leaderAndIsr.leader))
      })

    } catch {
      case e: Exception => {
        log.info("error while findPartitionsWithLeadersFromZk %1$s".format(e), e)
        throw e
      }
    }
    finally {
      zkClient.close()
    }
    result
  }

  /**
   * Find list of partitions for specified topic.
   *
   * @param zkQuorum  url or comma-separated list of urls of ZooKeeper instances
   * @param topic  topic to get partitions for
   * @param sessionTimeout  timeout of ZooKeeper session
   * @param connectionTimeout timeout for ZooKeeper request
   * @return sequence of Ints, representing partitions
   */
  def findPartitionsFromZk(zkQuorum: String, topic: String, sessionTimeout: Int = 10000, connectionTimeout: Int = 10000): Seq[Int] = {
    val result = mutable.MutableList[Int]()
    val zkClient = new ZkClient(zkQuorum, sessionTimeout, connectionTimeout, ZKStringSerializer)
    try {

      ZkUtils.getPartitionsForTopics(zkClient, Array(topic)).foreach(x => {
        x._2.foreach(y => {
          result += y
        })
      })

    } catch {
      case e: Exception => {
        log.info("error while findPartitionsFromZk %1$s".format(e), e)
        throw e
      }
    }
    finally {
      zkClient.close()
    }
    result
  }


  /**
   * Get iterator for data from (topic, partition) pair.
   * @param leader leader broker for partition as (broker-host, broker-port)
   * @param topic topic to read from
   * @param partition partition to read from
   * @param offset start message offset (as message ordinal number)
   * @param limit batch size in bytes
   * @param clientId client id
   * @param soTimeout passed directly to simple consumer
   * @param bufferSize: buffer size for simple consumer
   * @param maxWait: max wait time for simple consumer
   **/
  def getData(leader: (String, Int), topic: String, partition: Int, offset: Long, limit: Int, clientId: String = "", soTimeout: Int = 5000, bufferSize: Int = 5 * 1024 * 1024, maxWait: Int = 100): (Short, Option[Iterator[(Long, Long, Option[Array[Byte]], Array[Byte])]]) = {

    val leaderConsumer: SimpleConsumer = new SimpleConsumer(leader._1, leader._2, soTimeout, bufferSize, clientId)
    var iterator: Option[Iterator[(Long, Long, Option[Array[Byte]], Array[Byte])]] = None
    var status = ErrorMapping.NoError
    try {
      val result = getDataFromLeader(leaderConsumer, topic, partition, offset, limit, clientId, soTimeout, bufferSize, maxWait)
      status = result._1
      iterator = result._2
    } catch {
      case e: Exception => {
        status = ErrorMapping.UnknownCode
        log.error("" + e, e)
      }
    } finally {
      if (leaderConsumer != null) {
        leaderConsumer.close()
      }
    }
    (status, iterator)
  }


  def getDataFromLeader(leaderConsumer: SimpleConsumer, topic: String, partition: Int, offset: Long, limit: Int, clientId: String = "", soTimeout: Int = 5000, bufferSize: Int = 5 * 1024 * 1024, maxWait: Int = 100): (Short, Option[Iterator[(Long, Long, Option[Array[Byte]], Array[Byte])]]) = {

    val frBuilder = new FetchRequestBuilder()
    frBuilder.clientId(clientId).addFetch(topic, partition, offset, limit).maxWait(maxWait)
    val fetchRequest = frBuilder.build()

    var status: Short = ErrorMapping.NoError
    var iterator: Option[Iterator[(Long, Long, Option[Array[Byte]], Array[Byte])]] = None
    try {
      val fetchResponse = leaderConsumer.fetch(fetchRequest)

      if (fetchResponse.hasError) {
        status = fetchResponse.errorCode(topic, partition)
        log.error("error for topic %1$s partition %2$s = %3$s".format(topic, partition, status))
      } else {
        //collect data

        val frpd = fetchResponse.data.get(new TopicAndPartition(topic, partition))

        if (frpd.isDefined) {

          iterator = Some(new Iterator[(Long, Long, Option[Array[Byte]], Array[Byte])] {

            val data = frpd.get.messages.iterator

            def hasNext = data.hasNext

            def next(): (Long, Long, Option[Array[Byte]], Array[Byte]) = {
              val mo = data.next()
              val offset = mo.offset
              val nextOffset = mo.nextOffset
              val key = if (mo.message.hasKey) {
                val kbuf = Array.range(0, mo.message.keySize, 1).map(_.toByte)
                mo.message.key.get(kbuf)
                Some(kbuf)
              } else {
                None
              }
              val value = Array.range(0, mo.message.payload.limit(), 1).map(_.toByte)
              mo.message.payload.get(value)
              (offset, nextOffset, key, value)
            }
          })
        }

      }

    } catch {
      case e: Exception => {
        status = ErrorMapping.UnknownCode
        log.error("" + e, e)
      }
    }
    (status, iterator)
  }

}
