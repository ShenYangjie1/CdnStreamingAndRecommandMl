package utils

/**
  * Created by zengxiaosen on 2017/5/24.
  */
import java.io.Serializable

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPool, Response}
object RedisUtils extends Serializable{
  val redisHost: String = ConfigLoader.redisHost
  val redisPort: Int = ConfigLoader.redisPort
  val redisTimeout: Int = ConfigLoader.redisTimeout
  val password: String = ConfigLoader.redisPassWord
  val config = new GenericObjectPoolConfig()
  config.setMaxTotal(300)
  lazy val pool = new JedisPool(config, redisHost, redisPort, redisTimeout, password)
  lazy val hook = new Thread{
    override def run(): Unit = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run())

  /**
    * write the mapping of domain to ipSet to redis, Suitable for large quantities
    */

  def writeDomainIpToRedis(map: Map[String, scala.collection.mutable.Set[String]], redisPartition: Int): Unit = {
    lazy val jedis = RedisUtils.pool.getResource
    val pipeline = jedis.pipelined()
    jedis.select(redisPartition)
    //  jedis.flushDB() // delete the data of DB-0 for updata
    map.keys.foreach{ i =>
      val it = map(i).iterator
      while(it.hasNext){
        pipeline.expire(i, 60*60*24)
        pipeline.sadd(i, it.next())
      }
      pipeline.sync()
    }

  }
}
