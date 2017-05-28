package utils

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by zengxiaosen on 2017/5/23.
  */
object StreamUtils {

  def getDStreamFromSocket(ip: String, port: String, ssc: StreamingContext) = {
    ssc.socketTextStream(ip, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
  }

  def getDStreamFromKafka(brokers: String, topics: String, ssc: StreamingContext) = {
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )
  }

}
