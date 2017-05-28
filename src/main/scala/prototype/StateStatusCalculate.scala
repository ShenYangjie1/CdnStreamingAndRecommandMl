package prototype

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType
import utils.SchemaUtils

/**
  * Created by zengxiaosen on 2017/5/23.
  */
class StateStatusCalculate(rowRDD: RDD[Row], sqlContext: SQLContext) {
  val schema: StructType = SchemaUtils.getStatsSchema
  val statsDF: DataFrame = sqlContext.createDataFrame(rowRDD, schema)
  statsDF.registerTempTable("stats")

  private def businessDF: DataFrame = {
    sqlContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    "00000" as domainCode,
                    "00" as stateCode,
                    businessCode,
                    eventTS
                    FROM stats GROUP BY eventTS, businessCode""")
  }

  private def areaDF: DataFrame = {
    sqlContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    "00000" as domainCode,
                    stateCode,
                    "000" as businessCode,
                    eventTS
                    FROM stats GROUP BY  stateCode,eventTS""")
  }

  private def totalDF: DataFrame = {
    sqlContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    "00000" as domainCode,
                    "00" as stateCode,
                    "000" as businessCode,
                    eventTS
                    FROM stats GROUP BY eventTS""")
  }

  def unionAll(dataFrames: Seq[DataFrame], sqlContext: SQLContext): DataFrame = dataFrames match {
    case Nil => sqlContext.emptyDataFrame
    case head :: Nil => head
    case head :: tail => head.unionAll(unionAll(tail, sqlContext))
  }

  def outputDF(): DataFrame ={
    unionAll(Seq(areaDF, totalDF, businessDF, statsDF), sqlContext)
  }
}
