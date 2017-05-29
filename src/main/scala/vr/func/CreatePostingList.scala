package vr.func

/**
  * Created by zengxiaosen on 2017/5/29.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import utils.RedisUtils

import scala.collection.JavaConversions
//离线构造倒排记录表
object CreatePostingList {
  def main(args: Array[String]): Unit = {
    //实时获取数据出现重复数据
    val jedisPut = RedisUtils.pool.getResource
    val jedis = RedisUtils.pool.getResource
    jedis.select(7)
    val pipeline = jedisPut.pipelined()
    jedisPut.select(9)
    JavaConversions.asScalaSet(jedis.keys("*")).foreach(key=>{
      JavaConversions.asScalaBuffer(jedis.lrange(key,0,-1)).foreach(word=>{
        pipeline.lpush(word,key)
      })
    })
    pipeline.sync()
    jedisPut.close()
    jedis.close()
  }
}
