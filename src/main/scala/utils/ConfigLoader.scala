package utils

/**
  * Created by zengxiaosen on 2017/5/17.
  */
import java.util.Properties

import com.typesafe.config.ConfigFactory
object ConfigLoader {
  def mysqlUser: String = ConfigFactory.load().getString("mysql.user")

  def mysqlPwd: String = ConfigFactory.load().getString("mysql.pwd")

  def jdbcUrl: String = ConfigFactory.load().getString("mysql.jdbc")

  def mysqldb: String = ConfigFactory.load().getString("mysql.db")

  def appName: String = ConfigFactory.load().getString("app.name")

  def logtype: Int = ConfigFactory.load().getInt("log.logtype")


  def getMysqlProp: Properties = {
    val prop = new Properties()
    prop.put("user", mysqlUser)
    prop.put("password", mysqlPwd)
    prop
  }
}
