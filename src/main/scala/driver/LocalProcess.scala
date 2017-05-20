package driver

import entity.{IPRegion, NginxLogEvent, StatsRecord}
import extract.ETLProcess
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{BroadcastUtils, IPService, LocalDataUtils}

/**
  * Created by zengxiaosen on 2017/5/17.
  */
object LocalProcess {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("DataTest")
      .set("spark.default.parallelism", "108") //每个stage默认的task数量
      .set("spark.storage.memoryFraction", "0.7") //RDD cache可以使用的内存 默认0.6
      .set("spark.shuffle.file.buffer", "512k") //shuffle write的缓冲区大小
      .set("spark.reducer.maxSizeInFlight", "256M") //shuffle read 的缓冲区大小
      .set("spark.streaming.stopSparkContextByDefault", "true") //spark 任务优雅退出
      .set("spark.shuffle.memoryFraction", "0.5") //shuffle可使用的内存，默认0.2

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    process(sc,sqlContext)
  }


  def processShow(etlRdd: RDD[NginxLogEvent]): Unit = {
    print("====================== output ========================")
    val finalRdd = etlRdd.map(ETLProcess.map2KV(_))
    .reduceByKey((a: StatsRecord, b: StatsRecord) => StatsRecord.add(a, b), 108)
    .map(_._2)
    .map(x => Row(x.reqTime, x.bodySize, x.xx2, x.xx3, x.xx4, x.xx5, x.requestNum, x.domainCode, x.stateCode, x.ts))

    ETLProcess.showDataframe(finalRdd)
  }



  def process(sc: SparkContext, sQLContext: SQLContext): Unit = {
    val businessMap = BroadcastUtils.getBusinessMap(sc)
    println("businessMap: ")
    val businessMapValue = businessMap.value
    businessMapValue.map(ite =>{
      println("key: " + ite._1.toString + "  value: " + ite._2.toString)
    })
    println(businessMap.toString())
    println("======================")
    val ipArray = BroadcastUtils.getIpArray(sc)

    println("ipArray: ")
    val ipArrayValue = ipArray.value
    ipArrayValue.map(ite => {
      println("region: " + ite.region)
    })
    println("======================")
    val localdata = LocalDataUtils.getDataFromLocalFile(sc)
    println("fileData: ")

//    localdata.map(ite => {
//      println(ite.toString)
//    })
    println("======================")
    val etlRdd = processETL(localdata, businessMap, ipArray)
    //打印
    processShow(etlRdd)

  }



  def processETL(localdata: RDD[String], businessMap: Broadcast[Map[String, String]], ipArray: Broadcast[Array[IPRegion]]) = {
    val etlRdd = localdata.filter(_.length > 20)
      .map(ETLProcess.map2Event(_))
      .filter(ETLProcess.isFileter(_))
      .filter(ETLProcess.badlineFilter(_))
      .map(events => {
        val IpService = IPService(ipArray, 50000, true)
        ETLProcess.process(events, IpService, businessMap)
      })
    etlRdd
  }


}
