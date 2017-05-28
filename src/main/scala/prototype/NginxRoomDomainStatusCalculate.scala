package prototype

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.{DataFrameUtils, SchemaUtils}

/**
  * Created by zengxiaosen on 2017/5/23.
  */
class NginxRoomDomainStatusCalculate(rowRDD: RDD[Row], sQLContext: SQLContext) {

  val schema: StructType = SchemaUtils.getNginxRoomDomainStatsSchema
  val statsDF: DataFrame = sQLContext.createDataFrame(rowRDD, schema)
  statsDF.registerTempTable("nginxDomainStats")

  private def businessDF: DataFrame = {
    sQLContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    engineRoom,
                    "0000" as engine,
                    "00000" as domainCode,
                    "000" as businessCode,
                    0 as writeFlag,
                    eventTS
                    FROM nginxDomainStats GROUP BY eventTS, engineRoom"""
    )
  }

  private def areaDF: DataFrame = {
    sQLContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    "00" as engineRoom,
                    "0000" as engine,
                    "00000" as domainCode,
                    businessCode,
                    0 as writeFlag,
                    eventTS
                    FROM nginxDomainStats GROUP BY  businessCode,eventTS""")
  }

  def outputDF(): DataFrame ={
    DataFrameUtils.unionAll(Seq(businessDF,areaDF,statsDF), sQLContext)
  }



}
