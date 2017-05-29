package vr.datasource

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{ConfigLoader, RedisUtils}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.native.JsonMethods.parse
import org.json4s.JsonDSL._
import vr.entity.{NewVideo, VREvent}

import scala.collection.JavaConversions
import scala.util.parsing.json.JSON
/**
  * Created by zengxiaosen on 2017/5/26.
  */
object KafkaOperationtest {

  /*
  uv:count(distinct guid) group by date
  借助set完成去重复
  检查点存全部数据
   */


  def main(args: Array[String]): Unit = {


    val checkpointDirectory = "/user/xiaoliu/checkpoint"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        //val topics = "rdc_nginxlog"
        val topics = "vr_event"
        val brokers = "10.31.96.221:9092,10.31.96.222:9092,10.31.96.223:9092,10.31.96.224:9092,10.31.96.225:9092,10.31.96.226:9092,10.31.96.227:9092,10.31.96.228:9092,10.31.96.229:9092,10.31.96.230:9092"
        /*
brokers = "10.31.96.221:9092,10.31.96.222:9092,10.31.96.223:9092,10.31.96.224:9092,10.31.96.225:9092,10.31.96.226:9092,10.31.96.227:9092,10.31.96.228:9092,10.31.96.229:9092,10.31.96.230:9092"
  topics = "rdc_nginxlog"
 */
        val sparkConf = new SparkConf().
          setAppName(ConfigLoader.appName)
        val sc = new SparkContext(sparkConf)
        val sQLContext = new SQLContext(sc)
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.checkpoint(checkpointDirectory)

        //create direct kafka stream with brokers and topics
        /*
        直接消费是不经过zookeeper的,所以这时候你就要告诉我kafka的地址,而不是zookeeper的地址里
         */
        val topicSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        //stringdecoder表示传的消息是字符串类型的
        /*
        不经过zk直接消费kafka,而通常情况下还是会经过zookeeper
        因为经过zookeeper你直接告诉zookeeper的地址就行了
        但是如果你用direct就要告诉它broker的地址了
        经过前人大量实验,直接消费的稳定性会好一些
        但是从框架的结偶度来讲,经过zookeeper的耦合度会低一些
         */
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicSet
        )

        //computeUV(messages)
        writeVideo2Redis(messages)
        updateDictAndVideoProfile(messages)
        updateUserProfile(sc, sQLContext, messages)
        updatePostingList(sc, sQLContext, messages)
        writeEvent2Redis(messages)
        ssc
      })
  }

  //更新倒排记录表
  def updatePostingList(sc:SparkContext, sQLContext: SQLContext, kafkaMessages: InputDStream[(String, String)]): Unit ={
    val videoDstream = kafkaMessages.filter(_._2!=null).map(_._2).filter(_.length > 20).filter(_.contains("eventCategory")).filter(_.contains("videoTitle"))
    val videoRdd = videoDstream.map(line=>{
      val json = parse(line)
      implicit val formats = DefaultFormats
      val video = (json \ "eventBody").extract[NewVideo]
      video
    })

    // videoDstream.print(1000000)
    videoRdd.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedis = RedisUtils.pool.getResource
        val pipeline = jedis.pipelined()
        jedis.select(9)
        iter.foreach(video=>{
          video.videoTag.trim.split(",").foreach(word=>{
            if(word.trim!=""){
              pipeline.lpush(word.trim,video.id.toString)
            }
          })
          println(video.id)
        })
        pipeline.sync()
        jedis.close()
      })
    })
  }

  //更新用户画像--利用kafka中的event事件，以及离线的内容画像，构造用户画像（根据用户观看视频事件利用视频的标签给用户打上标签和相应的权值）
  def updateUserProfile(sc: SparkContext, sQLContext: SQLContext, kafkaMessages: InputDStream[(String, String)]): Unit ={
    //先过滤出有用的事件
    val eventDstream = kafkaMessages.filter(_._2!=null).map(_._2).filter(_.length>20).filter(_.contains("eventCategory")).filter(x=>{x.contains("\"play_video\"") || x.contains("\"share_video\"") || x.contains("\"collect\"")})
    val eventRdd = eventDstream.map(line=>{
      val json = parse(line)
      implicit val formats = DefaultFormats
      val event = (json \ "eventBody").extractOrElse[VREvent](VREvent.apply("","",""))
      (event.device_id,event.object_id,event.event)
    }).filter(_._1!="")

    //从redis中读取视频标签数据
    val jedis = RedisUtils.pool.getResource
    jedis.select(7)
    val keys = jedis.keys("*")
    val videoKeys = JavaConversions.asScalaSet(keys)
    val videoData = videoKeys.map(videoKey=>{
      (videoKey, JavaConversions.asScalaBuffer(jedis.lrange(videoKey, 0, -1)))
    }).toSeq
    jedis.close()

    val videoDF = sQLContext.createDataFrame(videoData).toDF("object_id", "tag_list")

    //将用户标签存储到redis，键为device_id,值为map类型（key为标签，值为权值)
    val event2Weight:Map[String, Double] = Map("play_video"->0.5,"collect"->0.1,"share_video"->0.1)
    eventRdd.foreachRDD(rdd=>{
      import sQLContext.implicits._
      val eventDF = rdd.toDF("device_id","object_id","event")
      val res = eventDF.join(videoDF, "object_id").cache()
      if(res.count()!=0){
        res.foreachPartition(iter=>{
          val jedis = RedisUtils.pool.getResource
          val pipeline = jedis.pipelined()
          jedis.select(8)
          iter.foreach(row=>{
            val tagList = JavaConversions.asScalaBuffer(row.getList[String](3))
            val deviceId = row.getAs[String]("device_id").replace(":","|")
            val weight = event2Weight(row.getAs[String]("event"))
            tagList.foreach(word=>{
              pipeline.hincrByFloat(deviceId, word, weight)
            })
          })
          pipeline.sync()
          jedis.close()//及时关闭，否则会阻塞
        })
        //res.show(25000)
      }
      //eventDF.join(videoDF,"object_id").show(30000)

    })



  }

  def updateDictAndVideoProfile(kafkaMessages: InputDStream[(String, String)]) {
    val videoDstream = kafkaMessages.filter(_._2 != null).map(_._2).filter(_.length > 20).filter(_.contains("eventCategory")).filter(_.contains("videoTitle"))
    val videoRdd =videoDstream.map(line=>{
      val json = parse(line)
      implicit val format = DefaultFormats
      val video = (json \ "eventBody").extract[NewVideo]
      //todo:进一步筛选，有可能有不符合格式的数据
      video
    })

    //利用视频的标签生成一个字典
    var count = 0
    videoRdd.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedisPut = RedisUtils.pool.getResource
        val jedisDict = RedisUtils.pool.getResource
        val pipeline = jedisPut.pipelined()
        jedisDict.select(5)
        jedisPut.select(2)
        iter.foreach(video=>{
          val wordList = video.videoTag.split(",")
          wordList.foreach(word=>{
            if(!word.trim.equals("")){
              if(jedisDict.setnx(word.trim, count.toString)==1){
                count+=1
                pipeline.lpush(video.id.toString, count.toString)
              }else{
                pipeline.lpush(video.id.toString, jedisDict.get(word.trim))
              }
            }
          })
        })
        pipeline.sync()
        jedisDict.close()
        jedisPut.close()
      })
    })
  }

  def writeVideo2Redis(kafkaMessages: InputDStream[(String, String)]) {
    //先将垃圾数据清洗掉,只获取视频信息
    val videoDstream = kafkaMessages.filter(_._2 != null).map(_._2).filter(_.length > 20).filter(_.contains("eventCategory")).filter(_.contains("videoTitle"))
    val videoRdd = videoDstream.map(line=>{
      val json = parse(line)
      implicit val formats=DefaultFormats
      val video = (json \ "eventBody").extract[NewVideo]
      video
    })

    //   提供标签语料
    videoRdd.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedis = RedisUtils.pool.getResource
        val pipeline = jedis.pipelined()
        jedis.select(6)
        iter.foreach(video=>{
          val json = ("video_id" -> video.id) ~ ("video_title" -> video.videoTitle) ~ ("video_url" -> video.videoUrl) ~ ("video_duration" -> video.videoDuration) ~ ("video_upload_time" -> video.videoUploadTime) ~ ("video_describe" -> video.videoDescribe) ~
            ("video_thumbnail_url" -> video.videoThumbnailUrl) ~ ("video_play_count" -> video.videoPlayCount) ~ ("video_size" -> video.videoSize) ~ ("video_width" -> video.videoHeight) ~ ("video_height" -> video.videoHeight) ~
            ("v_r_format" -> video.vRFormat) ~ ("ads_poster_url" -> video.adsPosterUrl) ~ ("ads_poster_describe" -> video.adsPosterDescribe) ~ ("video_type" -> video.videoType) ~ ("video_tag" -> video.videoTag) ~
            ("author_id" -> video.authorId) ~ ("video_category_id" -> video.videoCategoryId) ~ ("original_video" -> video.originalVideo) ~ ("like_count" -> video.likeCount) ~
            ("collect_count" -> video.collectCount) ~ ("summary" -> video.summary) ~ ("vertical_thumb" -> video.verticalThumb) ~ ("code" -> video.code)
          val jsonString = compact(render(json))
        })
        pipeline.sync()
        jedis.close()
      })
    })



  }

  def writeEvent2Redis(messages: InputDStream[(String, String)]) = {
    //过滤有用事件
    val sourceLogDstream = messages.filter(_._2!=null).map(_._2).filter(_.length>20).filter(_.contains("eventCategory"))
    .filter(x => {x.contains("\"play_video\"") || x.contains("\"share_video\"") || x.contains("\"collect\"")})
    val eventRdd = sourceLogDstream.map(line => {
      //val json = JSON.parseFull(line)
      val json = parse(line)
      implicit val formats = DefaultFormats
      val event = (json \ "eventBody").extractOrElse[VREvent](VREvent.apply("","",""))
      (event.device_id,event.object_id)
    }).filter(_._1!="")

//    eventRdd.foreachRDD(rdd=>{
    //      rdd.foreachPartition(iter=>{
    //        val jedis = RedisUtils.pool.getResource
    //        val pipeline = jedis.pipelined()
    //        jedis.select(10)
    //        iter.foreach{case(device_id, object_id)=>{
    //          pipeline.lpush(device_id.replace(":","|"),object_id)
    //        }}
    //        pipeline.sync()
    //        jedis.close()
    //      })
    //    })
    eventRdd.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        iter.foreach{
          case(device_id, object_id)=>{
            println("device_id: " + device_id + " object_id: " + object_id)
          }
        }
      })
    })


  }


  def computeUV(messages: InputDStream[(String, String)]) = {
    /*
    这个message里面是tuple2,第二列是数据
     */
    val sourceLog = messages.map(_._2)

    val utmUvLog = sourceLog.filter(_.split(",").size == 3).map(logInfo => {
      val arr = logInfo.split(",")
      val date = arr(0).substring(0, 10)
      val guid = arr(1)
      (date, Set(guid))
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    /*
    通过updatestatebykey进行聚合,聚合之后按照updateUvUtmCountState函数进行处理
     */
    val utmDayActive = utmUvLog.updateStateByKey(updateUvUtmCountState). //返回date,set(guid)
      //随意第一列result._1就是date,而result._2.size就是uv
      map(result => {
      (result._1, result._2.size.toLong)
    }).print()

  }

  def updateUvUtmCountState(values: Seq[Set[String]], state: Option[Set[String]]): Option[Set[String]] = {
    /*
    因为上一步是updatestatebykey,传进来的是value而没有key
    所以传进来的是Set(guid)
     */
    val defaultState = Set[String]()
    values match {
      //val Nil:scala.collection.immutable.Nil,它是一种特殊的类型,理解为空就行了
      //如果为空(首个批次)就返回一个空的state
      case Nil => Some(state.getOrElse(defaultState))
      //否则
      case _ =>
        //set与set拼接用++
        /*
        把seq里面所有的set拼接起来,它会自动完成去重
        set里面的元素自动进行去从,通过hashcode
         */
        val guidSet = values.reduce(_ ++ _)
        /*
        some其实就是option类型,返回的是一个包含全部guid的集合
         */
        println("11111-" + state.getOrElse(defaultState).size)
        println("22222-" + guidSet.size)
        Some(defaultState ++ guidSet)

    }
  }
}