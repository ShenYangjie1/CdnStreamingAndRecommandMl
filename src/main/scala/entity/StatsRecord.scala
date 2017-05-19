package entity

/**
  * Created by zengxiaosen on 2017/5/17.
  */
case class StatsRecord(var reqTime: Double, var bodySize: Long, var xx2: Long, var xx3: Long, var xx4: Long,
                       var xx5: Long, var requestNum: Long, var ts: Long, var domainCode: String,
                       var stateCode: String)

object StatsRecord {
  def apply(reqTime: Double,
            bodySize: Long,
            xx2: Long,
            xx3: Long,
            xx4: Long,
            xx5: Long,
            pv: Long,
            domainCode: String,
            stateCode: String,
            ts: Long
           ): StatsRecord = new StatsRecord(
    reqTime,
    bodySize,
    xx2,
    xx3,
    xx4,
    xx5,
    pv,
    ts,
    domainCode,
    stateCode)

  def add(a: StatsRecord, b: StatsRecord): StatsRecord = {
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