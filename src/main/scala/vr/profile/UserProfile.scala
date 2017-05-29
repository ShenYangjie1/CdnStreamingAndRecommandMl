package vr.profile

import org.apache.spark.SparkConf
import utils.RedisUtils
import vr.Utils.UserInfoUtils

import scala.collection.JavaConversions

/**
  * Created by zengxiaosen on 2017/5/29.
  */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
object UserProfile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("User Profile").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("data/user_sample.json")
    df.foreachPartition(iter=>{
      val jedis = RedisUtils.pool.getResource
      val pipeline = jedis.pipelined()
      jedis.select(0)
      iter.foreach(user=>{
        val id  = user.getAs[Long]("id")
        val email = user.getAs[String]("email")
        val phone = user.getAs[String]("phone")
        val name = user.getAs[String]("name")
        val sex = user.getAs[String]("sex")
        val avatar_url = user.getAs[String]("avatar_url")
        val time_stamp = user.getAs[Long]("time_stamp")
        val auth_id = user.getAs[Long]("auth_id")
        val introduce = user.getAs[String]("introduce")
        val birthday = user.getAs[String]("birthday")
        val last_read_sys_msg_time = user.getAs[Long]("last_read_sys_msg_time")
        val last_read_privet_msg_time = user.getAs[Long]("last_read_privet_msg_time")
        val user_type = user.getAs[Long]("user_type")
        val Array(area, mobileSimType, zipCode) = UserInfoUtils.getMobileInfo(phone)
        var age = 0
        var zodica = ""
        var constellation = ""
        if (birthday != "") {
          age = UserInfoUtils.getAgeFromBirth(birthday)
          zodica = UserInfoUtils.getZodica(birthday)
          constellation = UserInfoUtils.getConstellation(birthday)
        }


        val json = ("email" -> email) ~ ("phone" -> phone) ~ ("name" -> name) ~ ("sex" -> sex) ~ ("avatar_url" -> avatar_url) ~
          ("area" -> area) ~ ("mobileSimType" -> mobileSimType) ~ ("zipCode" -> zipCode) ~ ("time_stamp" -> time_stamp) ~
          ("auth_id" -> auth_id) ~ ("introduce" -> introduce) ~ ("birthday" -> birthday) ~ ("age" -> age) ~ ("zodica" -> zodica) ~
          ("constellation" -> constellation) ~ ("last_read_sys_msg_time" -> last_read_sys_msg_time) ~
          ("last_read_privet_msg_time" -> last_read_privet_msg_time) ~ ("user_type" -> user_type)

        val jsonString = compact(render(json))
        pipeline.mset(id.toString, jsonString)
      })
      pipeline.sync()
    })

    val jedis = RedisUtils.pool.getResource
    jedis.select(0)
    val keys = jedis.keys("*")
    val userKeys = JavaConversions.asScalaSet(keys)
    var responses = scala.collection.mutable.Map[String, String]()
    userKeys.foreach(userKey => {
      val userValue = jedis.get(userKey)
      responses += userKey -> userValue
    })
  }

}
