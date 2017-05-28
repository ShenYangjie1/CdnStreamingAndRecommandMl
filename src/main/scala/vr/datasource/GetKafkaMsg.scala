package vr.datasource

/**
  * Created by zengxiaosen on 2017/5/26.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.native.JsonMethods.parse
import org.json4s.JsonDSL._
import utils.{ConfigLoader, StreamUtils}
object GetKafkaMsg {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc,Seconds(ConfigLoader.batchInterval))
    ssc.checkpoint("/user/xiaoliu/kafka/checkpoint")
    process(sc,sqlContext,ssc)
    ssc.start()
    ssc.awaitTermination()
  }

  def process(sc: SparkContext, sqlContext: SQLContext, ssc: StreamingContext) = {
    /*
    duration="365 days"
    kafka {
    brokers = "10.31.96.221:9092"
    topics = "vr_event"
     }
     */
    val kafkaMessages = StreamUtils.getDStreamFromKafka(ConfigLoader.brokers,
      ConfigLoader.topics, ssc)
    writeEvent2Redis(kafkaMessages)

  }

  def writeEvent2Redis(kafkaMessages: InputDStream[(String, String)]): Unit ={
    //先过滤出有用的事件
    val eventDstream = kafkaMessages.filter(_._2!=null).map(iter => {
      val key = iter._1
      val value = iter._2

      println(key + " : " + value)
    })
  }


}
