package prototype

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.{DataFrameUtils, SchemaUtils}


/**
  * Created by zengxiaosen on 2017/5/24.
  */
class EngineStatusCalculate (rowRDD: RDD[Row], sQLContext: SQLContext){

  val schema: StructType = SchemaUtils.getEngineStatsSchema
  val statsDF: DataFrame = sQLContext.createDataFrame(rowRDD, schema)
  statsDF.registerTempTable("engineStats")

  private def engineRoomDF: DataFrame = {
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
                    "2" as logType,
                    0 as writeFlag,
                    eventTS
                    FROM engineStats GROUP BY eventTS, engineRoom"""
    )
  }

  private def room2EngineDF: DataFrame = {
    sQLContext.sql(
      """SELECT SUM(requestTime) as requestTime,
                    SUM(requestSize) as requestSize,
                    SUM(xx2) as xx2,
                    SUM(xx3) as xx3,
                    SUM(xx4) as xx4,
                    SUM(xx5) as xx5,
                    SUM(requestNum) as requestNum,
                    engineRoom,
                    engine,
                    "2" as logType,
                    0 as writeFlag,
                    "000000" as eventTS
                    FROM engineStats GROUP BY engineRoom, engine""")
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
                    engineRoom,
                    "0000" as engine,
                    logType,
                    0 as writeFlag,
                    eventTS
                    FROM engineStats GROUP BY eventTS,engineRoom,logType""")
  }

  def outputDF(): DataFrame = {
    DataFrameUtils.unionAll(Seq(engineRoomDF, totalDF, statsDF), sQLContext)
  }

  def room2EngineDFOut(): DataFrame = {
    room2EngineDF
  }




}
