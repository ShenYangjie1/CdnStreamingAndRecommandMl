package mr2sparkpro

import entity.Identity


/**
  * Created by zengxiaosen on 2017/5/22.
  */
case class RawDataEntity (var timeStr: String, var machine: String, var eventTs: String, var responseTime: String,
                          var srcIP: String, var status: String,
                          var bodySize: String, var method: String, var url: String,
                          var dstIP: String, var contentType: String, var referUrl: String,
                          var browser: String)

case class TempDataEntity(var dt_5m_millis: String, var requestNum: Long, var responseTimeStr: Double,
                         var bodySizeStr: Long, var XX2_Result: Long, var XX3_Result: Long, var XX4_Result: Long, var XX5_Result: Long)

object TempDataEntity {
  def add(x1: TempDataEntity, x2: TempDataEntity): TempDataEntity = {
    x1.requestNum += x2.requestNum
    x1.responseTimeStr += x2.responseTimeStr
    x1.bodySizeStr += x2.bodySizeStr
    x1.XX2_Result += x2.XX2_Result
    x1.XX3_Result += x2.XX3_Result
    x1.XX4_Result += x2.XX4_Result
    x1.XX5_Result += x2.XX5_Result
    x1
  }
}
