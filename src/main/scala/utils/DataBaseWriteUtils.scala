package utils

import entity.AlertEvent
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zengxiaosen on 2017/5/24.
  */
object DataBaseWriteUtils {
  def ServerWriteList(record: Row,map: Map[String, String], array: ArrayBuffer[AlertEvent]): Unit = {
    val ts = record.getAs[Long]("eventTS")
    if(System.currentTimeMillis() / 1000 - ts > 1800){
      val latency = record.getAs[Double]("requestTime")
      val flow = record.getAs[Long]("requestSize")
      val bandwidth = (flow / latency).toLong
      val request = record.getAs[Long]("requestNum")
      val XX4 = record.getAs[Long]("xx4")
      val XX5 = record.getAs[Long]("xx5")
      val dstIP = record.getAs[String]("dstIP")
      val serverRoom = record.getAs[String]("serverRoom")
      val writeFlag = record.getAs[Int]("writeFlag")
      var mysqlDstIP = ""

      if(dstIP.length > 12){
        mysqlDstIP = "000.000.000.000"
      }else{
        mysqlDstIP = dstIP
      }

      if(writeFlag == 1){
        //1---ngnix,2---cache_server,3---source_server，4---domain
        if(latency > map("latency").toDouble){
          var alertEvent = AlertEvent("", "", "", mysqlDstIP, "not Machine", ts.toString)
          if (serverRoom == "01") {
            alertEvent.eventType = "3"
          } else {
            alertEvent.eventType = "2"
          }
          alertEvent.param += "latency"
          alertEvent.value += latency.toString
          array += alertEvent
        }

        if (bandwidth < map("bandwidth").toDouble) {
          var alertEvent = AlertEvent("", "", "", mysqlDstIP, "not Machine", ts.toString)
          if (serverRoom == "01") {
            alertEvent.eventType = "3"
          } else {
            alertEvent.eventType = "2"
          }
          alertEvent.param += "bandwidth"
          alertEvent.value += bandwidth.toString
          array += alertEvent
        }

        if(flow < map("flow").toLong){
          var alertEvent = AlertEvent("", "", "", mysqlDstIP, "not Machine", ts.toString)
          if (serverRoom == "01") {
            alertEvent.eventType = "3"
          } else {
            alertEvent.eventType = "2"
          }
          alertEvent.param += "flow"
          alertEvent.value += flow.toString
          array += alertEvent
        }

        if (XX4 > map("XX4").toLong) {
          var alertEvent = AlertEvent("", "", "", mysqlDstIP, "not Machine", ts.toString)
          if (serverRoom == "01") {
            alertEvent.eventType = "3"
          } else {
            alertEvent.eventType = "2"
          }
          alertEvent.param += "XX4"
          alertEvent.value += XX4
          array += alertEvent
        }
        if (XX5 > map("XX5").toLong) {
          var alertEvent = AlertEvent("", "", "", mysqlDstIP, "not Machine", ts.toString)
          if (serverRoom == "01") {
            alertEvent.eventType = "3"
          } else {
            alertEvent.eventType = "2"
          }
          alertEvent.param += "XX5"
          alertEvent.value += XX5
          array += alertEvent
        }

        if(request < map("request").toLong){
          var alertEvent = AlertEvent("", "", "", mysqlDstIP, "not Machine", ts.toString)
          if (serverRoom == "01") {
            alertEvent.eventType = "3"
          } else {
            alertEvent.eventType = "2"
          }
          alertEvent.param += "request"
          alertEvent.value += request
          array += alertEvent
        }
      }
    }
  }

  def EngineWriteList(record: Row, threshold: Map[String, String], array: ArrayBuffer[AlertEvent]): Unit = {
    val ts = record.getAs[Long]("eventTS")
    if (System.currentTimeMillis() / 1000 - ts > Constants.AlertGap) {
      val flow = record.getAs[Long]("requestSize")
      val latency = record.getAs[Double]("requestTime")
      val bandwidth = (flow / latency).toLong
      val request = record.getAs[Long]("requestNum")
      val XX4 = record.getAs[Long]("xx4")
      val XX5 = record.getAs[Long]("xx5")
      val engineRoom = record.getAs[String]("engineRoom")
      val engine = record.getAs[String]("engine")
      val writeFlag = record.getAs[Int]("writeFlag")

      if (writeFlag == 1) {
        // 标志位为0是初始化，标志位为1表示可以写了，标志位为2表示写过了
        if (flow < threshold("flow").toLong) {
          var alertEvent = AlertEvent("1", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += "flow"
          alertEvent.value += flow.toString
          array += alertEvent
        }
        if (bandwidth < threshold("bandwidth").toLong) {
          var alertEvent = AlertEvent("1", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += "bandwidth"
          alertEvent.value += bandwidth.toString
          array += alertEvent
        }
      }
    }
  }

  def NginxRoomDomainWriteList(record: Row, map: Map[String, String], array: ArrayBuffer[AlertEvent]): Unit = {
    val ts = record.getAs[Long]("eventTS")
    if (System.currentTimeMillis() / 1000 - ts > 1800) {
      val flow = record.getAs[Long]("requestSize")
      val latency = record.getAs[Double]("requestTime")
      val bandwidth = (flow / latency).toLong
      val request = record.getAs[Long]("requestNum")
      val XX4 = record.getAs[Long]("xx4")
      val XX5 = record.getAs[Long]("xx5")
      val engineRoom = record.getAs[String]("engineRoom")
      val engine = record.getAs[String]("engine")
      val writeFlag = record.getAs[Int]("writeFlag")

      //eventType, param, value, dstIP, machineRoom, eventTime

      if (writeFlag == 1) {

        if (flow < map("flow").toLong) {
          var alertEvent = AlertEvent("4", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += "flow" + " "
          alertEvent.value += flow.toString + " "
          array += alertEvent
        }
        if (bandwidth < map("bandwidth").toLong) {
          var alertEvent = AlertEvent("4", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += bandwidth + " "
          alertEvent.value += bandwidth.toString + " "
          array += alertEvent
        }
        if (latency > map("latency").toDouble) {
          var alertEvent = AlertEvent("4", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += "latency"
          alertEvent.value += latency.toString
          array += alertEvent
        }

        if (XX4 > map("XX4").toLong) {
          var alertEvent = AlertEvent("4", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += "XX4"
          alertEvent.value += XX4
          array += alertEvent
        }
        if (XX5 > map("XX5").toLong) {
          var alertEvent = AlertEvent("4", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += "XX5"
          alertEvent.value += XX5
          array += alertEvent
        }
        if (request < map("request").toLong) {
          var alertEvent = AlertEvent("4", "", "", engine, engineRoom, ts.toString)
          alertEvent.param += "request"
          alertEvent.value += request
          array += alertEvent
        }
      }
    }
  }

  def room2IPRedis(roomIpDF: DataFrame): Unit = {
    val array = roomIpDF.map(row => {
      val dstIP = row.getAs[String]("dstIP")
      val serverRoom = row.getAs[String]("serverRoom")
      val set = scala.collection.mutable.Set[String]()
      set.add(dstIP)
      val map = Map("serverRoom"+serverRoom -> set)
      //      map.keys.foreach(x => println(x + ": " + map(x)))
      map
    }).collect()
    //      .foreachPartition(iter => {
    //      iter.foreach(map => {
    //        //        finalMap = (finalMap /: map1) { case (map, (k, v)) => map + (k -> (v ++ map.getOrElse(k, Set[String]()))) }
    //        RedisUtils.writeDomainIpToRedis(map, 0)
    //      })
    //    })
    var i = 0
    var finalMap = Map[String, scala.collection.mutable.Set[String]]()
    while (i < array.length) {
      finalMap = (finalMap /: array(i)) { case (map, (k, v)) => map + (k -> (v ++ map.getOrElse(k, Set[String]()))) }
      i += 1
    }
    //    println("array length: "+array.length)
    //    println(array)
    //    println("map size: "+finalMap.size)
    //    println(finalMap)
    //RedisUtils.writeDomainIpToRedis(finalMap, 1)
    //    finalMap.keys.foreach(x => println(x + ": " + finalMap(x)))
  }


  def room2EngineRedis(roomIpDF: DataFrame): Unit = {
    val array = roomIpDF.map(row => {
      val engine = row.getAs[String]("engine")
      val engineRoom = row.getAs[String]("engineRoom")
      val set = scala.collection.mutable.Set[String]()
      set.add(engine)
      val map = Map("nginxRoom"+engineRoom -> set)
      map
    }).collect()

    var i = 0
    var finalMap = Map[String, scala.collection.mutable.Set[String]]()
    while (i < array.length) {
      finalMap = (finalMap /: array(i)) { case (map, (k, v)) => map + (k -> (v ++ map.getOrElse(k, Set[String]()))) }
      i += 1
    }
    //RedisUtils.writeDomainIpToRedis(finalMap, 1)
  }

}
