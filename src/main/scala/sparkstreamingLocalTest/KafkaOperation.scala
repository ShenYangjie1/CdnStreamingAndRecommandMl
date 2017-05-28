package sparkstreamingLocalTest

import java.util.Properties

import entity.{NginxLogEvent, StatsRecord, StatusRecord1}
import extract.{DataBaseWriteProcess, ETLProcess}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import prototype.{EngineStatusCalculate, NginxRoomDomainStatusCalculate}
import utils._

/**
  * Created by zengxiaosen on 2017/5/23.
  */
object KafkaOperation {

  val writeGap: Int = ConfigLoader.writeGap
  val schema: StructType = SchemaUtils.getSchema
  val statsSchema: StructType = SchemaUtils.getStatsSchema
  val mysqlProp: Properties = ConfigLoader.getMysqlProp
  val batchInterval: Int = ConfigLoader.batchInterval



  def main(args: Array[String]) {
    def functionToCreateContext(): StreamingContext = {
      // If you do not see this printed, that means the StreamingContext has been loaded
      // from the new checkpoint
      val sparkConf = new SparkConf().
        setAppName(ConfigLoader.appName)
        .set("spark.serializer",
          "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[NginxLogEvent], classOf[StatsRecord]))
        //        .set("spark.streaming.concurrentJobs", "4")
        .set("spark.default.parallelism", "108") //每个stage默认的task数量
        //spark 1.6之后的内存管理
        .set("spark.memory.fraction", "0.8") //Fraction of (heap space - 300MB) used for execution and storage
        .set("spark.memory.storageFraction", "0.2") //storage/(storage+execution)
        .set("spark.shuffle.file.buffer", "512k") //shuffle write的缓冲区大小
        .set("spark.reducer.maxSizeInFlight", "256M") //shuffle read 的缓冲区大小
        .set("spark.streaming.stopSparkContextByDefault", "true") //spark 任务优雅退出
        //spark 1.6之前的内存管理
        //        .set("spark.shuffle.memoryFraction", "0.5") //shuffle可使用的内存，默认0.2
        //        .set("spark.storage.memoryFraction", "0.5") //RDD cache可以使用的内存 默认0.6
        //spark 数据本地性配置
//        .set("spark.locality.wait.node", "10000")
//        .set("spark.locality.wait.process", "10000")
//        .set("spark.locality.wait.rack", "10000")
//        .set("spark.speculation", "true")

      val sc = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sc)
      //val ssc = new StreamingContext(sc, Seconds(ConfigLoader.batchInterval))
      val ssc = new StreamingContext(sc, Seconds(2));
      ssc.checkpoint(ConfigLoader.checkpoint)
      process1(sc, ssc, sqlContext)
      ssc
    }
    val ssc = StreamingContext.getOrCreate(ConfigLoader.checkpoint, functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def process1(sc: SparkContext, ssc: StreamingContext, sqlContext: SQLContext): Unit = {
    val kafkaMessages = StreamUtils.getDStreamFromKafka(ConfigLoader.brokers,
      ConfigLoader.topics, ssc)
    val lines = kafkaMessages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
    .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    //    val etlRdd = processETL(kafkaMessages)
//    etlRdd.cache()
//    processRdd(etlRdd)
    //    val etlRdd = EtlProcessStream(kafkaMessages)``
    //etlRdd.cache()
    /*
    processStateStatus(etlRdd)
    processEngineXStatus(etlRdd)
    processServerStatus(etlRdd)
    processNginxRoomDomainStatus(etlRdd)
     */
    //    processStateStatus(etlRdd)
    //    processEngineXStatus(etlRdd)


  }


  def processEngineXStatus(etlRdd: DStream[NginxLogEvent1]): Unit = {
    etlRdd.map(x => Map2KVUtils.EngineMap2KV(x))
    .reduceByKey((a: StatusRecord1, b: StatusRecord1) => StatusRecord1.add(a, b), 108)
    .updateStateByKey(updateStatistics)
    .map(_._2)
    .map(x => Row(x.reqTime, x.bodySize, x.xx2, x.xx3, x.xx4, x.xx5, x.requestNum, x.engineRoom, x.engine, x.logType, x.writeFlag, x.ts))
    .foreachRDD(rdd => {
      //ETL处理过后的writeFlag在聚合数据里怎么算？
      //暂时不统计聚合数据的报警事件，统一将writeFlag置为0或者2（不为1即可）
      val sQLContext = SQLContext.getOrCreate(rdd.context)
      val unionDF = new EngineStatusCalculate(rdd, sQLContext).outputDF()
      unionDF.show(30)
      val room2EngineDF = new EngineStatusCalculate(rdd, sQLContext).room2EngineDFOut()
      room2EngineDF.show(1000)
      DataBaseWriteUtils

    })

  }

  def State2KV(x: NginxLogEvent1) : (String, StatusRecord1) = {
    (DateUtils.round2Mins(x.eventTs.toDouble.toLong, 5).toString
      + x.businessLine.domainCode + x.location.stateCode,
      StatusRecord1(x.responseTime.toDouble, x.bodySize.toLong,
        x.statusCode.xx2.toLong, x.statusCode.xx3.toLong, x.statusCode.xx4.toLong, x.statusCode.xx5.toLong, 1,
        x.businessLine.domainCode, x.location.stateCode, x.businessLine.business, x.dstIP, x.engine.engineRoom, x.engine.serverRoom,x.engine.engine, x.logType, 0,
        DateUtils.round2Mins(x.eventTs.toDouble.toLong, 1))
    )
  }

  def processRdd(etlRdd: DStream[NginxLogEvent1]): Unit = {
    etlRdd.map(x => State2KV(x)).foreachRDD(iter=>{
      val result = iter.collect()
      println("输出啦!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      result.toList.map(y=>{
        println(y._1)
        println("hhhhhh")
      })
    })
  }

  def process(sc: SparkContext, ssc: StreamingContext, sqlContext: SQLContext): Unit = {
    val kafkaMessages = StreamUtils.getDStreamFromKafka(ConfigLoader.brokers,
    ConfigLoader.topics, ssc)
    val etlRdd = processETL(kafkaMessages)
    etlRdd.cache()
    processRdd(etlRdd)
//    val etlRdd = EtlProcessStream(kafkaMessages)``
    //etlRdd.cache()
    /*
    processStateStatus(etlRdd)
    processEngineXStatus(etlRdd)
    processServerStatus(etlRdd)
    processNginxRoomDomainStatus(etlRdd)
     */
//    processStateStatus(etlRdd)
//    processEngineXStatus(etlRdd)


  }

  private def processETL(kafkaMessages: InputDStream[(String, String)]) = {
    val etlRdd = kafkaMessages
      .filter(_._2.length > 20)
      .map(_._2)
      .map(x => ETLProcessUpdate.map2Event(x))
      .filter(x => ETLProcessUpdate.isFilter(x))
      .filter(x => ETLProcessUpdate.badlineFilter(x))
      .mapPartitions(events => {
        val staticMap: Map[String, Int] = MysqlUtils.getStaticCacheMap()
        val businessMap = MysqlUtils.getBusinessMap1()
        val ipArray = MysqlUtils.getIpArray()
        val IpService = IPService(ipArray, 50000, true)
        events.map(e => ETLProcessUpdate.process(e, IpService, businessMap, staticMap))
      })
    etlRdd
  }


  def processStateStatus(etlRdd: DStream[NginxLogEvent1]): Unit = {
    etlRdd.map(x => Map2KVUtils.StateMap2KV(x))
      .reduceByKey((a: StatusRecord1, b: StatusRecord1) => StatusRecord1.add(a, b), 108) //reduce by minute signiture
      //          .mapWithState(StateSpec.function(zjmapping)).stateSnapshots()
      .updateStateByKey(updateStatistics)
      .map(_._2)
      .map(x => Row(x.reqTime, x.bodySize, x.xx2, x.xx3, x.xx4, x.xx5, x.requestNum, x.domainCode, x.stateCode, x.businessCode, x.ts))
      .foreachRDD(
        x => DataBaseWriteProcess.hBaseWrite(x)
      )
  }

  // merge the min statistic with the previous batch result
  // discard policy: discard all the time-out records
  def updateStatistics(input: Seq[StatusRecord1], state: Option[StatusRecord1]): Option[StatusRecord1] = {
    var result: StatusRecord1 = null
    // no new record in
    if(input.isEmpty){
      result = state.get
    }
    // Now because we used the reduce function before this function we are
    // only ever going to get at most one event in the Sequence
    input.foreach(c =>{
      if(state.isEmpty){
        //a totally new record
        result = c
      }else{
        result = StatusRecord1.add(state.get, c)
      }
    })
    //当标志位（writeFlag)为0的时候，表示还没有写过，需要判断是否满足时间间隔，若满足
    //时间间隔则将标志位置为1
    //当标志位不为0的时候，表示数据已经写过，置为2
    if(state.isDefined){
      if(result.writeFlag == Constants.InitialState){
        if(System.currentTimeMillis()/1000 - state.get.writeFlag > Constants.AlertGap){
          result.writeFlag = Constants.WritableState
        }
      }else{
        result.writeFlag = Constants.WrittenState
      }
    }

    if(result != null){
      if(System.currentTimeMillis() / 1000 - result.ts > writeGap){
        return None
      }
    }
    Some(result)
  }

  def EtlProcessStream(kafkaMessages: InputDStream[(String, String)]) = {
    val etlRdd = kafkaMessages.filter(_._2.length > 20)
    .mapPartitions(x => {
      x.map(_._2)
    })
    .mapPartitions(x => {
      x.map(y => Process.map2Event(y))
    })
    .filter(x => Process.isFilter(x))
    .filter(x => Process.badlineFilter(x))
    .mapPartitions(events =>{
      val statisMap: Map[String, Int] = MysqlUtils.getStaticCacheMap()
      val businessMap = MysqlUtils.getBusinessMap1()
      val ipArray = MysqlUtils.getIpArray()
      val IpService1 = IPService_stream(ipArray, 5000, true)
      events.map(e => Process.process(e, IpService1, businessMap, statisMap))
    })
    etlRdd

  }


}
