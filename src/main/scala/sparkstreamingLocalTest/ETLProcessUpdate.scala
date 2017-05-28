package sparkstreamingLocalTest

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import utils.{ConfigLoader, IPService}

import scala.io.Source
import scala.util.control.Breaks

/**
  * Created by zengxiaosen on 2017/5/26.
  */
object ETLProcessUpdate {
  private val ipMap = Map[String, String](
    "BeiJing" -> "01", "NeiMengGu" -> "02", "ShanXi" -> "03",
    "HeBei" -> "04", "TianJin" -> "05", "NingXia" -> "06",
    "ShaanXi" -> "07", "GanSu" -> "08", "QingHai" -> "09",
    "XinJiang" -> "10", "HeiLongJiang" -> "11", "JiLin" -> "12",
    "LiaoNing" -> "13", "FuJian" -> "14", "JiangSu" -> "15",
    "AnHui" -> "16", "ShanDong" -> "17", "ShangHai" -> "18",
    "ZheJiang" -> "19", "HeNan" -> "20", "HuBei" -> "21",
    "JiangXi" -> "22", "HuNan" -> "23", "GuiZhou" -> "24",
    "YunNan" -> "25", "ChongQing" -> "26", "SiChuan" -> "27",
    "XiZang" -> "28", "GuangDong" -> "29", "GuangXi" -> "30",
    "HaiNan" -> "31"
  )

  private val roomMap = Map[String,String](
    "BJ" -> "01", "sy" -> "02", "tc" -> "03",
    "zw" -> "04", "zz" -> "05", "ah" -> "06",
    "hbtt" -> "07", "bx" -> "08", "jn4" -> "09",
    "cd" -> "10", "nj2" -> "11", "xa" -> "12",
    "gz-rm" -> "13", "sh5" -> "14", "yd" -> "15",
    "GZ" -> "16", "zjm" -> "17", "hk" -> "18",
    "wh2" -> "19", "dxt3" -> "20","cta" -> "21"
  )

  private val least = 0.0000001


  def process(e: NginxLogEvent1, ipService: IPService, businessMap: Map[String, (String, String)], staticMap: Map[String, Int]): NginxLogEvent1 = {

    //    StringUtils.replace(e.referUrl, "\"", "") //去除referurl中的""号
    e.status = StringUtils.replace(e.status, "-/", "")
    if (e.contentType != null && e.contentType != "") {
      e.contentType = StringUtils.split(e.contentType, ';')(0) //这里需要处理一下 总报空指针异常
    }


    if ((e.responseTime.toDouble >= -least) && (e.responseTime.toDouble <= least)) {
      e.responseTime = "0.001"
    }

    if (StringUtils.contains(e.url, "http")) {
      businessMatch(e, businessMap)
    }



    calculate(e)

    if (e.machine.contains("local_nginx@") && e.machine.contains("_")) {
      //      val str1 = e.machine.substring(12)
      val str1 = StringUtils.substring(e.machine, 12)
      //      val str2 = str1.split("_")(0)
      val str2 = StringUtils.split(str1, "_")(0)
      e.engine.engine = str1
      e.engine.engineRoom = roomMap.getOrElse(str2,"99") //如果map中没有就是99
    }

    if (e.dstIP.length > 8) {
      val ip = StringUtils.substring(e.dstIP, 7)
      e.dstIP = StringUtils.split(ip, ":")(0)
    }

    if (staticMap.getOrElse(e.dstIP, 0) == 1) {
      e.logType = "0"
      e.engine.serverRoom = e.engine.engineRoom
    } else {
      e.logType = "1"
      e.engine.serverRoom = "01"
    }

    e.location.state = ip2state(e.srcIP, ipService)

    e.location.stateCode = ipMap.getOrElse(e.location.state, "99")

    //    val datePartition = DateUtils.calculateDatePartition(new Timestamp(
    //      e.eventTs.toDouble.toLong * 1000))
    //    e.datePartition.year = datePartition._1
    //    e.datePartition.month = datePartition._2
    //    e.datePartition.day = datePartition._3
    //    e.datePartition.hour = datePartition._4

    e
  }

  def isFilter(e: NginxLogEvent1): Boolean = e != null

  //  def staticFilter(etlRdd:DStream[NginxLogEvent]) = {
  //    etlRdd.filter(e=>{
  //      !(e.url.contains(".js")||e.url.contains(".jpg")||e.url.contains(".gif")||e.url.contains(".png")||e.url.contains(".css")
  //        ||e.contentType.contains("image")||e.contentType.contains("javascript")||e.contentType.contains("css"))
  //    })
  //  }

  def calculate(e: NginxLogEvent1): Unit = {
    val status = e.status.toInt
    if (status >= 500) {
      e.statusCode.xx5 = 1
    }
    else if (status >= 400) {
      e.statusCode.xx4 = 1
    }
    else if (status >= 300) {
      e.statusCode.xx3 = 1
    }
    else if (status >= 200) {
      e.statusCode.xx2 = 1
    }
  }

  //过滤格式不规范的数据
  def badlineFilter(e: NginxLogEvent1): Boolean = {
    (StringUtils.contains(e.url, "http://") || StringUtils.contains(e.url, "https://")) &&
      (StringUtils.contains(e.method, "GET") || StringUtils.contains(e.method, "POST")) &&
      isNumber(e.eventTs) && isNumber(e.bodySize) && isNumber(e.responseTime)
  }

  """
    This will split on one or more spaces only if those spaces are followed by zero,
    or an even number of quotes (all the way to the end of the string!).
  """

  def getSplitPattern: String = {
    var splitPattern = ""
    if (ConfigLoader.logtype == 1) {
      splitPattern = "[ ]+(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    } else {
      splitPattern = " "
    }
    splitPattern
  }

  //val splitPattern = "[ ]+(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

  """
    This function is to extract and fill the fields of each record
  """.stripMargin


  def formatLine(e: String): String = {
    StringUtils.replace(e, "; ", ";")
  }

  def parseCookie(cookie_str: String): Map[String, String] = {
    StringUtils.split(cookie_str, ";").map(e => e.split("=") match {
      case Array(f1, f2) => (f1, f2)
      case _ => ("-", "-") // add empty string handle
    }).toMap
  }


  def ip2state(ip: String, iPService: IPService): String = {
    iPService.getLocationWithLruCache(ip) match {
      case Some(state) => state
      case None => "other"
    }
  }


  /**
    * 根据匹配规则，设置相应的destbusiness值
    *
    * @param e   NginxLogEvent
    * @param map 读取配置文件映射而成的匹配规则
    * @return
    */
  def businessMatch(e: NginxLogEvent1, map: Map[String, (String, String)]): NginxLogEvent1 = {
    try {
      var host = getHost(e.url)
      val length = StringUtils.split(host, '.').length
      for (i <- 0 until length - 1) {
        val value = map.getOrElse(host, ("99999", "999"))
        if (value != ("99999", "999")) {
          e.businessLine.domainCode = value._1
          e.businessLine.domain = host
          e.businessLine.business = value._2
          return e
        } else {
          host = StringUtils.substringAfter(host, ".")
        }
      }
    } catch {
      case ex: Exception =>
        println("URL exception: " + ex.toString + " " + e.url)
    }
    e.businessLine.domainCode = "99999"
    e.businessLine.business = "999"
    e.businessLine.domain = "other"
    e
  }

  //读取配置文件，形成Map匹配规则
  def getMap(filepath: String): Map[String, String] = {

    var map = Map[String, String]()
    val lines = Source.fromFile(filepath).getLines()

    lines.foreach(
      process
    )

    def process(str: String): Map[String, String] = {
      val array = StringUtils.split(str, "\t")
      map += (array(0) -> array(1))
      map
    }

    map
  }

  def map2Event(e: String): NginxLogEvent1 = {
    formatLine(e)
    val x = e.split(getSplitPattern)
    if (x.length > 12) {
      if (ConfigLoader.logtype == 0) {
        NginxLogEvent1(x(1), x(2), x(3),
          x(4), x(5), x(6), x(7), x(8),
          x(10), x(11), x(12), "", "")
      } else {
        NginxLogEvent1("", x(0), x(1), x(2), x(3),
          x(4), x(5), x(6), x(7), x(9),
          x(10), x(11), x(12))
      }
    }
    else null
  }

  def map2Row(etlRdd: DStream[NginxLogEvent1]): DStream[Row] = {
    etlRdd.map(e => Row(e.machine, e.eventTs.toDouble, e.responseTime.toDouble, e.srcIP, e.status.toInt,
      e.bodySize.toInt, e.method, e.url, e.dstIP, e.contentType,
      e.referUrl, e.location.country, e.location.stateCode, e.location.city, e.datePartition.year, e.datePartition.month, e.datePartition.day, e.datePartition.hour,
      e.businessLine.business, e.pv,
      e.userAgent, e.cookie, e.identity.uvID, e.identity.ipLOC, e.uagent.agent, e.uagent.os, e.uagent.device, e.statusCode.xx4, e.statusCode.xx5, e.location.stateCode, e.businessLine.domainCode
    ))
  }

  def eventWrite(rdd: RDD[Row], schema: StructType): Unit = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    val eventDF = sqlContext.createDataFrame(rdd, schema)
    eventDF
      .write.mode(SaveMode.Append)
      .partitionBy("year", "month", "day", "hour")
      .parquet(ConfigLoader.outputDir)
  }

  def getHost(str: String): String = {
    val charArray = str.toCharArray
    var count = 0
    var left = 0
    var right = 1
    val break = Breaks
    val stringBuilder = new StringBuilder()
    break.breakable {
      for (i <- 0 until charArray.length - 1) {
        if (charArray(i) == '/') {
          count = count + 1
          if (count == 2) {
            left = i
          }
          if (count == 3) {
            right = i
          }
          if (count >= 4) {
            break.break()
          }
        }
      }
    }
    for (j <- left + 1 until right) {
      stringBuilder.append(charArray(j))
    }
    //    str.substring(left+1,right)
    stringBuilder.toString()
  }

  def isNumber(s: String): Boolean = s.matches("\\d+.?\\d*")
}
