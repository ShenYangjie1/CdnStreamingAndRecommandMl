package entity

/**
  * Created by zengxiaosen on 2017/5/23.
  */
case class StatusRecord1(var reqTime: Double, var bodySize: Long, var xx2: Long, var xx3: Long, var xx4: Long,
                         var xx5: Long, var requestNum: Long, var ts: Long, var domainCode: String,
                         var stateCode: String, var businessCode: String, var dstIP: String, var engineRoom: String,var serverRoom: String,
                         var engine: String, var logType: String,var writeFlag:Int)


object StatusRecord1 {
  def apply(reqTime: Double,
            bodySize: Long,
            xx2: Long,
            xx3: Long,
            xx4: Long,
            xx5: Long,
            pv: Long,
            domainCode: String,
            stateCode: String,
            businessCode: String,
            dstIP: String,
            engineRoom: String,
            serverRoom: String,
            engine: String,
            logType: String,
            writeFlag: Int,
            ts: Long
           ): StatusRecord1 = new StatusRecord1(
    reqTime,
    bodySize,
    xx2,
    xx3,
    xx4,
    xx5,
    pv,
    ts,
    domainCode,
    stateCode,
    businessCode,
    dstIP,
    engineRoom,
    serverRoom,
    engine,
    logType,
    writeFlag)

  def add(a: StatusRecord1, b: StatusRecord1): StatusRecord1 = {
    a.reqTime += b.reqTime
    a.bodySize += b.bodySize
    a.requestNum += b.requestNum
    a.xx2 += b.xx2
    a.xx3 += b.xx3
    a.xx4 += b.xx4
    a.xx5 += b.xx5
    a
  }
}