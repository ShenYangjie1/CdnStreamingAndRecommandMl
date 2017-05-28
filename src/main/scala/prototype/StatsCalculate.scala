package prototype

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.SchemaUtils

/**
  * Created by zengxiaosen on 2017/5/20.
  */
class StatsCalculate (rowRDD: RDD[Row], sQLContext: SQLContext){
  val schema: StructType = SchemaUtils.getStatsSchema
  val statsDF: DataFrame = sQLContext.createDataFrame(rowRDD, schema)
  statsDF.registerTempTable("stats")

  private def areaDF: DataFrame = {
    sQLContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    "00000" as domainCode,
                    stateCode,
                    eventTS
                    FROM stats GROUP BY eventTS, stateCode"""
    )
  }

  private def totalDF: DataFrame = {
    sQLContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    "00000" as domainCode,
                    "00" as stateCode,
                    eventTS
                    FROM stats GROUP BY eventTS"""
    )
  }

  def unionAll(dataFrame: Seq[DataFrame], sQLContext: SQLContext) : DataFrame = dataFrame match {
    case Nil => sQLContext.emptyDataFrame
    case head :: Nil => head
    case head :: tail => head.unionAll(unionAll(tail,sQLContext))
  }

  def outputDF(): DataFrame = {
    unionAll(Seq(areaDF, totalDF, statsDF), sQLContext)
  }

}
