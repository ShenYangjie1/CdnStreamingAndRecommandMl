package vr.func

/**
  * Created by zengxiaosen on 2017/5/29.
  */
import breeze.linalg.SparseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{SparseVector => SV}

import scala.collection.JavaConversions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import utils.RedisUtils
object RecommendDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RecommendDemo")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val device_id="00:08:22:0e:3f:85"
    //recommendByTraverse(sc,device_id)
    recommendByPostingList(sc,device_id)
  }

  def recommendByPostingList(sc: SparkContext,device_id:String){
    //从redis中找到对应设备号的标签序列
    val jedis=RedisUtils.pool.getResource
    jedis.select(8)
    val device_id_trans=device_id.replace(":","|")
    val device_map=JavaConversions.mapAsScalaMap(jedis.hgetAll(device_id_trans))
    jedis.select(9)
    val videoScore=device_map.toSeq.flatMap(tag_weight=>{
      var list:List[(String,Double)] =List()
      JavaConversions.asScalaBuffer(jedis.lrange(tag_weight._1,0,-1)).foreach(video=>{
        list:+=(video,tag_weight._2.toDouble)
      })
      list
    })

    sc.parallelize(videoScore).reduceByKey(_+_).sortBy(_._2,false).collect().foreach(println(_))
  }


}
