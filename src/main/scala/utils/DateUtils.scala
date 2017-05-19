package utils

import java.sql.Timestamp

import org.joda.time.DateTime
/**
  * Created by zengxiaosen on 2017/5/17.
  */
object DateUtils {
  val offset = 8 * 60 * 60
  def calculateDatePartition(timestamp: Timestamp): (Int, Int, Int, Int) = {
    new DateTime(timestamp) match {
      case d: DateTime =>
        (d.getYear, d.getMonthOfYear, d.getDayOfMonth, d.getHourOfDay)
    }
  }

  def parseTimeStamp2Min(timestamp: String): String = {
    new DateTime(
      new Timestamp(timestamp.toDouble.toLong * 1000))
      .toString("yyyy/MM/dd HH:mm")
  }

  def removeTailingSec(ts: Long): Long = {
    round2Mins(ts, 1)
  }

  def round2Mins(ts: Long, n:Int):Long = {
    (ts/(60 * n)) * 60 * n
  }

  def genDayTS(ts:Long):Int = {
    ((ts + offset)/(60 * 60 * 24)).toInt
  }

  def gen5MinsKey(ts:Long): Int = {
    ((ts + offset) % (24 * 60 * 60) / (60 * 5)) .toInt +  1
  }
}
