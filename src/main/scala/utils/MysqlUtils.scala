package utils

import java.sql.{DriverManager, ResultSet}

import entity.IPRegion

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zengxiaosen on 2017/5/17.
  */
object MysqlUtils {
  val conn_str:String = ConfigLoader.jdbcUrl+ConfigLoader.mysqldb+"?user="+ConfigLoader.mysqlUser+"&password="+ConfigLoader.mysqlPwd
  //Load the driver
  classOf[com.mysql.jdbc.Driver]

  def getBusinessMap(): Map[String, String] = {
    val conn = DriverManager.getConnection(conn_str)
    var map = Map[String, String]()
    try{
      //Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM business_line")
      //Iterator Over ResultSet
      while(rs.next){
        val domain_name = rs.getString("domain_name")
        val domain_code = rs.getString("domain_code")
        map += (domain_code -> domain_name)
      }
    }
    finally {
      conn.close()
    }
    map
  }

  def getIpArray(): Array[IPRegion] = {
    val conn = DriverManager.getConnection(conn_str)
    val array =   ArrayBuffer[IPRegion]()
    try{
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM ipseg_new")
      // Iterate Over ResultSet
      while(rs.next){
        val minIP = ipToLong(rs.getString("minIP"))
        val maxIP = ipToLong(rs.getString("maxIP"))
        val region = rs.getString("region")
        val iPRegion = IPRegion(minIP, maxIP, region)
        array += iPRegion
      }
      array.toArray
    }finally {
      conn.close()
    }
  }

  def ipToLong(ipAddress: String): Long = {
    try {
      ipAddress.split("\\.").reverse.zipWithIndex.map(a => a._1.toInt * math.pow(256, a._2).toLong).sum
    } catch {
      case _: Throwable => 0
    }
  }
}
