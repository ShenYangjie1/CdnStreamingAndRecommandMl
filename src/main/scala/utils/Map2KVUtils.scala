package utils

import entity.{NginxLogEvent, StatusRecord1}
import sparkstreamingLocalTest.NginxLogEvent1


/**
  * Created by zengxiaosen on 2017/5/23.
  */
object Map2KVUtils {
  def EngineMap2KV(x: NginxLogEvent1): (String, StatusRecord1) = {
    (DateUtils.round2Mins(x.eventTs.toDouble.toLong, 1).toString + x.engine.engineRoom + x.engine.engine + x.logType,
    StatusRecord1(x.responseTime.toDouble, x.bodySize.toLong,
    x.statusCode.xx2.toLong, x.statusCode.xx2.toLong, x.statusCode.xx4.toLong, x.statusCode.xx5.toLong, 1,
      x.businessLine.domainCode, x.location.stateCode, x.businessLine.business, x.dstIP, x.engine.engineRoom, x.engine.serverRoom,x.engine.engine, x.logType, 0,
      DateUtils.round2Mins(x.eventTs.toDouble.toLong, 1)))
  }


  def StateMap2KV(x: NginxLogEvent1): (String, StatusRecord1) = {
    (DateUtils.round2Mins(x.eventTs.toDouble.toLong, 5).toString
       + x.businessLine.domainCode + x.location.stateCode,
    StatusRecord1(x.responseTime.toDouble, x.bodySize.toLong,
      x.statusCode.xx2.toLong, x.statusCode.xx3.toLong, x.statusCode.xx4.toLong, x.statusCode.xx5.toLong, 1,
      x.businessLine.domainCode, x.location.stateCode, x.businessLine.business, x.dstIP, x.engine.engineRoom, x.engine.serverRoom,x.engine.engine, x.logType, 0,
      DateUtils.round2Mins(x.eventTs.toDouble.toLong, 1)))

  }
}
