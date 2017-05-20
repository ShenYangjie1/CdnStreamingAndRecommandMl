package utils

import org.apache.spark.sql.types._

/**
  * Created by zengxiaosen on 2017/5/20.
  */
object SchemaUtils {

  val f1 = StructField("machine", StringType, nullable = true )
  val f2 = StructField("evenTs", DoubleType, nullable = true)
  val f3 = StructField("responseTime", DoubleType, nullable = true)
  val f4 = StructField("srcIP", StringType, nullable = true)
  val f5 = StructField("status", IntegerType, nullable = true)
  val f6 = StructField("bodySize", IntegerType, nullable = true)
  val f7 = StructField("method", StringType, nullable = true)
  val f8 = StructField("url", StringType, nullable = true)
  val f9 = StructField("dstIP", StringType, nullable = true)
  val f10 = StructField("contentType", StringType, nullable = true)
  val f11 = StructField("referUrl", StringType, nullable = true)
  val f12 = StructField("country", StringType, nullable = true)
  val f13 = StructField("state", StringType, nullable = true)
  val f14 = StructField("city", StringType, nullable = true)
  val f15 = StructField("year", IntegerType, nullable = true)
  val f16 = StructField("month", IntegerType, nullable = true)
  val f17 = StructField("day", IntegerType, nullable = true)
  val f18 = StructField("hour", IntegerType, nullable = true)
  val f19 = StructField("business", StringType, nullable = true)
  val f20 = StructField("pv", LongType, nullable = true)
  //表示是不是pageView
  val f21 = StructField("userAgent", StringType, nullable = true)
  val f22 = StructField("cookie", StringType, nullable = true)
  val f23 = StructField("uvID", StringType, nullable = true)
  val f24 = StructField("ipLOC", StringType, nullable = true)
  val f25 = StructField("agent", StringType, nullable = true)
  val f26 = StructField("os", StringType, nullable = true)
  val f27 = StructField("device", StringType, nullable = true)
  val f28 = StructField("pmFlag", StringType, nullable = true)//标志是移动端还是PC端
  val f29 = StructField("xx4", IntegerType, nullable = true)
  val f30 = StructField("xx5", IntegerType, nullable = true)
  val f31 = StructField("stateCode", StringType, nullable = true)
  val f32 = StructField("domainCode", StringType, nullable = true)

  def getSchema: StructType = {
    val array = Array[StructField](f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30, f31, f32)
    val schema = DataTypes.createStructType(array)
    schema
  }

  def getStatsSchema: StructType = {
    StructType(Seq(
      StructField("requestTime", DoubleType, nullable = true),
      StructField("requestSize", LongType, nullable = true),
      StructField("xx2", LongType, nullable = true),
      StructField("xx3", LongType, nullable = true),
      StructField("xx4", LongType, nullable = true),
      StructField("xx5", LongType, nullable = true),
      StructField("requestNum", LongType, nullable = true),
      StructField("domainCode", StringType, nullable = true),
      StructField("stateCode", StringType, nullable = true),
      StructField("eventTS", LongType, nullable = true)
    ))
  }

}
