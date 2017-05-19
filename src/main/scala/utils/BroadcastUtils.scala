package utils

import entity.IPRegion
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by zengxiaosen on 2017/5/17.
  */
object BroadcastUtils {
  @volatile private var businessMap: Broadcast[Map[String, String]] = null
  @volatile private var ipArray: Broadcast[Array[IPRegion]] = null

  def getBusinessMap(sc: SparkContext) : Broadcast[Map[String, String]] = {
    if(businessMap == null){
      synchronized{
        if(businessMap == null){
          businessMap = sc.broadcast(MysqlUtils.getBusinessMap())
        }
      }
    }
    businessMap
  }

  def getIpArray(sc: SparkContext): Broadcast[Array[IPRegion]] = {
    if(ipArray == null){
      synchronized{
        if(ipArray == null){
          ipArray = sc.broadcast(MysqlUtils.getIpArray())
        }
      }
    }
    ipArray
  }
}
