package sparkstreamingLocalTest


import org.apache.commons.lang.StringUtils
import utils.{ConfigLoader, IPService_stream}

import scala.util.control.Breaks

/**
  * Created by zengxiaosen on 2017/5/23.
  */
object Process {
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
  def process(e: NginxLogEvent1, IpService1: IPService_stream, businessMap:  Map[String, (String, String)], statisMap: Map[String, Int]): NginxLogEvent1 = {
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

    if(e.machine.contains("local_nginx@") && e.machine.contains("_")){
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

    if (statisMap.getOrElse(e.dstIP, 0) == 1) {
      e.logType = "0"
      e.engine.serverRoom = e.engine.engineRoom
    } else {
      e.logType = "1"
      e.engine.serverRoom = "01"
    }

    e.location.state = ip2state(e.srcIP, IpService1)
    e.location.stateCode = ipMap.getOrElse(e.location.state, "99")

    e
  }

  def ip2state(ip: String, iPService: IPService_stream): String = {
    iPService.getLocationWithLruCache(ip) match {
      case Some(state) => state
      case None => "other"
    }
  }

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

  //过滤格式不规范的数据
  def badlineFilter(e: NginxLogEvent1): Boolean = {
    (StringUtils.contains(e.url, "http://") || StringUtils.contains(e.url, "https://")) &&
      (StringUtils.contains(e.method, "GET") || StringUtils.contains(e.method, "POST")) &&
      isNumber(e.eventTs) && isNumber(e.bodySize) && isNumber(e.responseTime)
  }
  def isNumber(s: String): Boolean = s.matches("\\d+.?\\d*")
  def isFilter(x: NginxLogEvent1): Boolean = x != null

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

  def getSplitPattern: String = {
    var splitPattern = ""
    if (ConfigLoader.logtype == 1) {
      splitPattern = "[ ]+(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    } else {
      splitPattern = " "
    }
    splitPattern
  }

  def formatLine(e: String): String = {
    StringUtils.replace(e, "; ", ";")
  }

}
