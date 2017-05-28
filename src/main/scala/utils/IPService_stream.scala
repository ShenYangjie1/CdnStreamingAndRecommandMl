package utils

import com.twitter.util.{LruMap, SynchronizedLruMap}
import entity.IPRegion

/**
  * Created by zengxiaosen on 2017/5/23.
  */
class IPService_stream(ipArray: Array[IPRegion], lruCache: Int, synchronized: Boolean) {
  //  private val array = initLookUpService()
  private val array = ipArray
  private val lru = if (lruCache > 0) chooseAndCreateNewLru else null

  def chooseAndCreateNewLru = if (synchronized) new SynchronizedLruMap[Long, Option[String]](lruCache)
  else new LruMap[Long, Option[String]](lruCache)

  //  private def initLookUpService(): Array[IPRegion] ={
  //    val lines = Source.fromFile(filePath).getLines()
  //    val arraBuf = ArrayBuffer[IPRegion]()
  //    for (line <- lines){
  //      val array = line.split("\t")
  //      val iPRegion = IPRegion(IPService.ipToLong(array(0)),
  //        IPService.ipToLong(array(1)), array(2))
  //      arraBuf.append(iPRegion)
  //    }
  //    arraBuf.toArray
  //  }

  def getLocationWithLruCache(address: String): Option[String] = {
    val ip = IPService.ipToLong(address)
    lru.get(ip) match {
      case Some(loc) => loc
      case None =>
        val state = binarySearchIP(ip)
        val loc = Some(state)
        lru.put(ip, loc)
        loc
    }
  }

  def binarySearchIP(ip: Long): String = {
    var left = 0
    var right = array.length
    var mid = -1
    //    var count =  0
    //    println(ipInt, right)
    while (left < right) {
      mid = (left + right) / 2
      if (ip > array(mid).maxIP) {
        left = mid + 1
        //        println(">",count, left, mid, right, IPService.longToIP(array(mid).maxIP))
      } else if (ip < array(mid).minIP) {
        right = mid
        //        println("<",count, left, mid, right, IPService.longToIP(array(mid).maxIP))
      } else {
        return array(mid).region
      }
    }
    "other"
  }
}

object IPService_stream {

  def ipToLong(ipAddress: String): Long = {
    try {
      ipAddress.split("\\.").reverse.zipWithIndex.map(a => a._1.toInt * math.pow(256, a._2).toLong).sum
    } catch {
      case _: Throwable => 0
    }
  }

  def longToIP(long: Long): String = {
    (0 until 4).map(a => long / math.pow(256, a).floor.toInt % 256).reverse.mkString(".")
  }

  def apply(ipArray: Array[IPRegion], lruCache: Int = 10000, synchronized: Boolean):
  IPService_stream = new IPService_stream(ipArray, lruCache, synchronized)

}
