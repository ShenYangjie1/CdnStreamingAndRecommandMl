package vr.func
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import utils.RedisUtils

import scala.collection.JavaConversions
/**
  * Created by zengxiaosen on 2017/5/30.
  */
object Video2User {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Video2User")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //从json中读取事件数据（event，device_id，object_id）,暂时不考虑时间的衰减对标签权值的影响
    val df = sqlContext.read.json("data/event_sample_filter.json")
    val eventData = df.map(event=> {
      val eventName = event.getAs[String]("event")
      val deviceId = event.getAs[String]("device_id")
      val objectId = event.getAs[String]("object_id")
      (eventName, deviceId, objectId)
    }).filter(x=>{x._1.trim.length>0 && x._2.trim.length>0 && x._3.trim.length>0})
    //.filter(_._2.trim.length>0).filter(_._3.trim.length>0)
    //    eventData.foreach(event =>{
    //      println(event._1+" "+event._2+" "+event._3)
    //    })

    val eventDF=sqlContext.createDataFrame(eventData).toDF("event","device_id","object_id")
    //    eventDF.show()

    //从redis中读取视频标签数据
    val jedis = RedisUtils.pool.getResource
    jedis.select(2)
    val keys = jedis.keys("*")
    val videoKeys = JavaConversions.asScalaSet(keys)
    val videoData=videoKeys.map(videoKey=>{
      (videoKey, JavaConversions.asScalaBuffer(jedis.lrange(videoKey,0,-1)))
    }).toSeq
    jedis.close()
    val videoDF=sqlContext.createDataFrame(videoData).toDF("object_id","tag_list")

    //将用户标签存储到redis，键为device_id，值为map类型(Key为标签，值为权值)
    val event2Weight:Map[String,Double] = Map("play_video" -> 0.5 ,"collect"->0.1 ,"share_video"->0.1)
    eventDF.join(videoDF,"object_id").foreachPartition(iter =>{
      val jedis = RedisUtils.pool.getResource
      val pipeline = jedis.pipelined()
      jedis.select(3)
      iter.foreach(row=>{
        val tagList= JavaConversions.asScalaBuffer(row.getList[String](3))
        val deviceId=row.getAs[String]("device_id").replace(":","|")
        val weight=event2Weight(row.getAs[String]("event"))
        tagList.foreach(word => {
          pipeline.hincrByFloat(deviceId, word, weight)
        })
      })
      pipeline.sync()
      jedis.close()    //及时关闭，否则会阻塞
    })

    eventDF.join(videoDF,"object_id").show(1000)
  }

}
