package sparkstreamingLocalTest
import com.sun.xml.internal.ws.api.pipe.Engine

/**
  * Created by zengxiaosen on 2017/5/23.
  */
/**
  *1487233467.995
  *0.005
  *111.161.59.195
  * -/200
  * 591
  * GET
  * http://changyan.sohu.com/api/gold/user/get_coins?callback=jQuery1706211163070984185_1487233464670&client_id=cysfFizOu&_=1487233467763
  * -
  * DIRECT/10.16.39.68:80(0.001)
  * application/x-javascript; charset=UTF-8
  * "http://m.biquge.cc/html/9/9378/"
  * "Mozilla/5.0(Linux; U; Android 6.0.1; zh-cn; OPPO A57 Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/37.0.0.0 MQQBrowser/7.2 Mobile Safari/537.36"
  * "debug_uuid=C766D1F3A3F00001133BA550DF646770; sohucookie=thirdparty; debug_test=sohu_third_cookie"
  * @
  ***/
case class NginxLogEvent1(var machine: String,
                         var eventTs: String, var responseTime: String,
                         var srcIP: String, var status: String,
                         var bodySize: String, var method: String, var url: String,
                         var dstIP: String, var contentType: String, var referUrl: String,
                         var userAgent: String, var cookie: String,
                         var engine: Engine,
                         var identity: Identity,
                         var location: Location,
                         var uagent: UAgent,
                         var datePartition: DatePartition,
                         var businessLine: BusinessLine,
                         var pv: Long, //1为pageView，0则不是pageView
                         //                         var pmFlag: String,
                         var statusCode: StatusCode,
                         var logType: String)

case class Identity(var uvID: String, var ipLOC: String)

case class Location(var country: String, var state: String, var city: String, var stateCode:String)

case class UAgent(var agent: String, var os: String, var device: String)

case class DatePartition(var year: Int, var month: Int, var day: Int, var hour: Int)

case class BusinessLine(var business: String, var domain:String, var domainCode:String)

case class StatusCode(var xx2:Int, var xx3:Int, var xx4:Int, var xx5:Int)

case class Engine(var engineRoom: String, var engine: String,var serverRoom:String)

object NginxLogEvent1 {
  val DEFAULT_STR = ""
  val DEFAULT_NUM = 0

  def apply(machine: String,
            eventTs: String,
            responseTime: String,
            srcIP: String,
            status: String,
            bodySize: String,
            method: String,
            url: String,
            dstIP: String,
            contentType: String,
            referUrl: String,
            userAgent: String,
            cookie: String
           ): NginxLogEvent1 = new NginxLogEvent1(
    machine,
    eventTs, responseTime, srcIP, status, bodySize, method, url,
    dstIP, contentType, referUrl,
    userAgent, cookie,
    Engine(DEFAULT_STR, DEFAULT_STR,DEFAULT_STR),
    Identity(DEFAULT_STR, DEFAULT_STR),
    Location(DEFAULT_STR, DEFAULT_STR, DEFAULT_STR, DEFAULT_STR),
    UAgent(DEFAULT_STR, DEFAULT_STR, DEFAULT_STR),
    DatePartition(0, 0, 0, 0),
    BusinessLine(DEFAULT_STR, DEFAULT_STR, DEFAULT_STR),
    DEFAULT_NUM,
    //    pmFlag = "",
    StatusCode(0, 0, 0, 0),
    logType = ""
  )
}

