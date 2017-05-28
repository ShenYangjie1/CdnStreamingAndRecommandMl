package sparkstreamingLocalTest

//import com.twitter.util.{LruMap, SynchronizedLruMap}
//import com.twitter.util.{LruMap, SynchronizedLruMap}
import entity.IPRegion
import org.apache.spark.broadcast.Broadcast
import utils.MapMaker

/**
  * Created by zengxiaosen on 2017/5/17.
  */
class IPService (ipArray: Array[IPRegion], lruCache: Int, synchronized: Boolean){

  private val array = ipArray
  private val lru = if (lruCache > 0) chooseAndCreateNewLru2 else null
  def chooseAndCreateNewLru2 = if (synchronized) new scala.collection.mutable.HashMap[Long,Option[String]]
  else MapMaker.makeMap
//  def chooseAndCreateNewLru = if (synchronized) new SynchronizedLruMap[Long, Option[String]](lruCache)
//  else new LruMap[Long, Option[String]](lruCache)

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
    while(left < right){
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

object IPService{
  def ipToLong(ipAddress: String): Long = {
    try{
      ipAddress.split("\\.").reverse.zipWithIndex.map(a => a._1.toInt * math.pow(256,a._2).toLong).sum
    }catch {
      case _: Throwable => 0
    }
  }
  def longToIp(long: Long): String = {
    (0 until 4).map(a => long / math.pow(256, a).floor.toInt % 256).reverse.mkString(".")
  }

  def apply(ipArray: Array[IPRegion], lruCache: Int = 10000, synchronized: Boolean):
  IPService = new IPService(ipArray, lruCache, synchronized)}
