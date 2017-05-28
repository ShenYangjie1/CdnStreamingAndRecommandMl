package mr2sparkpro

import java.util.regex.Pattern

import entity.IPRegion
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import utils.{BroadcastUtils, ConfigLoader, LocalDataUtils}

/**
  * Created by zengxiaosen on 2017/5/22.
  */
object mrOperation {

  def GenerateSc(): SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName("DataTest")
      .set("spark.default.parallelism", "108") //每个stage默认的task数量
      .set("spark.storage.memoryFraction", "0.7") //RDD cache可以使用的内存 默认0.6
      .set("spark.shuffle.file.buffer", "512k") //shuffle write的缓冲区大小
      .set("spark.reducer.maxSizeInFlight", "256M") //shuffle read 的缓冲区大小
      .set("spark.streaming.stopSparkContextByDefault", "true") //spark 任务优雅退出
      .set("spark.shuffle.memoryFraction", "0.5") //shuffle可使用的内存，默认0.2

    val sc = new SparkContext(sparkConf)
    sc
  }

  def OutputToHdfs(etlRdd: RDD[(String, TempDataEntity)]) = {
    val resultRdd = etlRdd.map(x =>{

      val value_ = x._2.dt_5m_millis+"|"+x._2.bodySizeStr+"|"+x._2.requestNum+"|"+x._2.responseTimeStr+"|"+x._2.XX2_Result+"|"+x._2.XX3_Result+"|"+x._2.XX4_Result+"|"+x._2.XX5_Result
      println(x._1)
      println(value_)
      (x._1, value_)
    })

    resultRdd.saveAsTextFile("/user/xiaoliu/datajudgeresult/output.log")
  }

  def main(args: Array[String]): Unit = {
    val sc = GenerateSc()
    val sQLContext = new SQLContext(sc)
    val OUTVALUE_SEPA = "|"
    var errorCauseMissField = 0
    var errorCauseNotNum = 0
    var errorCauseStatusCode = 0
    var errorCauseURL = 0
    var errorCauseMethod = 0
    var totalNum = 0

    val localdata = LocalDataUtils.getDataFromLocalFile(sc)

    val regex = "(.*\\d+:\\d+:\\d+)? ?(\\S+[@| ]\\S+) (\\d+.?\\d+) (\\d+.?\\d+)" +
      " (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) (-/\\d+) (\\d+) (\\w{3,6})" +
      " (\\w{3,5}://\\S+) - (\\S+) (\\S+[;|; ]?[^\"]*?[;|; ]?[^\"]*?)" +
      " ([\"]\\S*[\"])( [\"].*[\"])?";

    val pattern = Pattern.compile(regex)

    val businessMap = BroadcastUtils.getBusinessMap(sc)
    val ipArray = BroadcastUtils.getIpArray(sc)

    val etlRdd = processOfEtl(businessMap, ipArray, localdata, pattern)
    etlRdd.persist(StorageLevel.MEMORY_AND_DISK)
    if(etlRdd == null){
      println("error ...")
    }

    OutputToHdfs(etlRdd)
  }


  def Map2Entity(s: String, pattern: Pattern): RawDataEntity = {
    val matcher = pattern.matcher(s)


    RawDataEntity(matcher.group(1),matcher.group(2),matcher.group(3),matcher.group(4),
        matcher.group(5),matcher.group(6),matcher.group(7),matcher.group(8),
        matcher.group(9),matcher.group(10),matcher.group(11),matcher.group(12),matcher.group(13))


    null
  }

  def FilWith2345(entity: RawDataEntity): Boolean = {
    val mystatus = StringUtils.removeStart(entity.status, "-/")
    // 状态码不是 2XX,3XX,4XX,5XX 是非法的日志
    if (!StringUtils.startsWith(mystatus, "2") && !StringUtils.startsWith(mystatus, "3") && !StringUtils.startsWith(mystatus, "4") && !StringUtils.startsWith(mystatus, "5")) {
      false
    } else true
  }

  def FilWithHttp(entity: RawDataEntity): Boolean = {
    val url = entity.url
    // 必须是http或https
    if (!StringUtils.contains(url, "http://") && !StringUtils.contains(url, "https://")) {
      false
    }else true
  }

  def FilMethod(entity: RawDataEntity): Boolean = {
    val method = entity.method
    // 只处理GET和POST,HEAD,DELETE, OPTIONS
    if (!StringUtils.contains(method, "GET") && !StringUtils.contains(method, "POST") && !StringUtils.contains(method, "HEAD") && !StringUtils.contains(method, "DELETE") && !StringUtils.contains(method, "OPTIONS")) {
      false
    }else true
  }

  def processOfEtl(businessMap: Broadcast[Map[String, String]], ipArray: Broadcast[Array[IPRegion]], localdata: RDD[String], pattern: Pattern) : RDD[(String, TempDataEntity)]  = {
    val etlRdd = localdata.filter(_.length > 20).filter(pattern.matcher(_).find()).map(Map2Entity(_, pattern)).filter(FilWith2345(_)).filter(FilWithHttp(_))
      .filter(FilMethod(_)).map(x => {
      val tsInSecond = x.timeStr.toDouble //
      val statusCode = StringUtils.removeStart(x.status, "-/")
      val interval_5m = 5 * 60
      val roundTs_5m = ((Math.floor(tsInSecond / interval_5m)) * interval_5m * 1000).toLong
      val dt_5m = new DateTime(roundTs_5m)
      val requestNum = 1L
      var XX2_Result = 0L
      var XX3_Result = 0L
      var XX4_Result = 0L
      var XX5_Result = 0L
      if (StringUtils.startsWith(statusCode, "2")) XX2_Result = 1
      else if (StringUtils.startsWith(statusCode, "3")) XX3_Result = 1
      if (StringUtils.startsWith(statusCode, "4")) XX4_Result = 1
      if (StringUtils.startsWith(statusCode, "5")) XX5_Result = 1

      //key
      val keyFinal = dt_5m.getMillis.toString
      //value
      val valueFinal = TempDataEntity(dt_5m.getMillis.toString, requestNum, x.responseTime.toDouble, x.bodySize.toLong,
        XX2_Result, XX3_Result, XX4_Result, XX5_Result)
      (keyFinal, valueFinal)
    }).reduceByKey((x1: TempDataEntity, x2: TempDataEntity) => TempDataEntity.add(x1, x2))

    etlRdd
  }

}
