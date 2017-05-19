package utils

import scala.collection.mutable

/**
  * Created by zengxiaosen on 2017/5/19.
  */
object MapMaker {
  def makeMap: mutable.HashMap[Long, Option[String]] with mutable.SynchronizedMap[Long, Option[String]] = {
    val a = new mutable.HashMap[Long, Option[String]] with mutable.SynchronizedMap[Long, Option[String]]
    a
  }
}
