package vr.func

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtils
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import vr.Utils.ChineseHandle
import vr.entity.Video

import scala.collection.JavaConversions

/**
  * Created by zengxiaosen on 2017/5/29.
  */
object CreateDict {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Video Cluster")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jedis = RedisUtils.pool.getResource
    jedis.select(6)
    val keys = jedis.keys("*")
    val videoKeys = JavaConversions.asScalaSet(keys)
    val responses = scala.collection.mutable.Map[String, String]()
    videoKeys.foreach(videoKey=>{
      val videoValue = jedis.get(videoKey)
      responses += videoKey -> videoValue
    })

    val dataSet = responses.map(item => {
      val videoProfile = parse(item._2, useBigDecimalForDouble = false)
      implicit val formats = DefaultFormats
      val video = videoProfile.extract[Video]
      //利用标题，描述，标签所有信息
      val content1 = ChineseHandle.getWordStr(video.video_title+video.video_describe+video.video_tag).split(" ")
      //根据标题，描述，标签提取关键字后的信息
      val content2 = ChineseHandle.chineseKeyWordCompute(ChineseHandle.getWordStr(video.video_title+video.video_tag),
        ChineseHandle.getWordStr(video.video_title+video.video_tag+video.video_describe), 10).split(" ")

      //只用标签信息
      val content = video.video_tag.split(",")
      (item._1, video.video_title, content)
    })

//    // TODO: 利用StringIndexer来生成字典
//    var wordList: List[String] = List()
//    dataSet.foreach(item=>{
//      wordList = wordList ::: item._3
//    })
//    wordList.foreach(println(_))

    jedis.select(5)
    //利用视频的标签生成一个字典
    var count=0
    dataSet.foreach(video =>{
      video._3.foreach(word =>{
        if(video._3.length>0){
          video._3.foreach(word =>{
            if(word.trim!=""){
              if(jedis.setnx(word.trim,count.toString)==1){
                count+=1
              }
            }
          })
        }
      })
    })

  }
}
