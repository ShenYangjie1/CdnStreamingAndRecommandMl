package extract

import entity.{NginxLogEvent, StatsRecord}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import prototype.StatsCalculate
import utils.{ConfigLoader, DateUtils, IPService}

import scala.util.control.Breaks

/**
  * Created by zengxiaosen on 2017/5/17.
  */
object  ETLProcess {
  def showDataframe(rdd: RDD[Row]) = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    val unionDF = new StatsCalculate(rdd, sqlContext).outputDF()

    println(unionDF.count())
    unionDF.show(10)
    //print for debug

  }

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
  private val least = 0.0000001

  def map2Event(e: String): NginxLogEvent = {
    formatLine(e)
    val x = e.split(getSplitPattern)
    if (x.length > 12) {
      if (ConfigLoader.logtype == 0) {
        NginxLogEvent(x(1), x(2), x(3),
          x(4), x(5), x(6), x(7), x(8),
          x(10), x(11), x(12), "", "")
      } else {
        NginxLogEvent("", x(0), x(1), x(2), x(3),
          x(4), x(5), x(6), x(7), x(9),
          x(10), x(11), x(12))
      }
    }
    else null
  }

  def formatLine(e: String): String = {
    StringUtils.replace(e, "; ","; ")
  }

  def getSplitPattern: String = {
    var splitPattern = ""
    if(ConfigLoader.logtype == 1){
      splitPattern = "[ ]+(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    }else{
      splitPattern = " "
    }
    splitPattern
  }



  def isFileter(e: NginxLogEvent) = e !=null

  //过滤格式不规范的数据
  def badlineFilter(e: NginxLogEvent): Boolean = {
    (StringUtils.contains(e.url, "http://") || StringUtils.contains(e.url, "https://")) &&
      isNumber(e.eventTs) && isNumber(e.bodySize) && isNumber(e.responseTime)
  }

  def isNumber(s: String): Boolean = s.matches("\\d+.?\\d*")


  def map2KV(x: NginxLogEvent) = {
    (DateUtils.round2Mins(x.eventTs.toDouble.toLong, 5).toString
      + x.businessLine.domainCode + x.location.stateCode,
      StatsRecord(x.responseTime.toDouble, x.bodySize.toLong,
        x.statusCode.xx2.toLong, x.statusCode.xx3.toLong,
        x.statusCode.xx4.toLong, x.statusCode.xx5.toLong, 1,
        x.businessLine.domainCode, x.location.stateCode, DateUtils.round2Mins(x.eventTs.toDouble.toLong, 5)))
  }

  def process(e: NginxLogEvent, ipService: IPService, businessMap: Broadcast[Map[String, String]]): NginxLogEvent = {
    e.status = StringUtils.replace(e.status, "-/", "")
    if(e.contentType != null && e.contentType != ""){
      e.contentType = StringUtils.split(e.contentType, ';')(0)
    }

    if ((e.responseTime.toDouble >= -least) && (e.responseTime.toDouble <= least)) {
      e.responseTime = "0.001".toString
    }

    if (StringUtils.contains(e.url, "http")) {
      businessMatch(e, businessMap.value)
    }

    calculate(e)
    e.location.state = ip2state(e.srcIP, ipService)

    e.location.stateCode = ipMap.getOrElse(e.location.state, "99")
    e
  }


  def ip2state(ip: String, iPService: IPService): String = {
    iPService.getLocationWithLruCache(ip) match {
      case Some(state) => state
      case None => "other"
    }
  }

  def calculate(e: NginxLogEvent) = {
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

  /**
    * 根据匹配规则，设置相应的destbusiness值
    *
    * @param e
    * @param map 读取配置文件映射而成的匹配规则
    * @return
    */
  def businessMatch(e: NginxLogEvent, map: Map[String, String]): NginxLogEvent = {
    try {
      var host = getHost(e.url)
      val length = StringUtils.split(host, '.').length
      for (i <- 0 until length - 1) {
        val value = map.getOrElse(host, "99999")
        if (value != "99999") {
          e.businessLine.domainCode = value
          e.businessLine.domain = host
          e.businessLine.business = host
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
    e.businessLine.domain = "other"
    e
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

}
