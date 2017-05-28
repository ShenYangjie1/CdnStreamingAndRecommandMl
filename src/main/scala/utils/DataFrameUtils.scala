package utils

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by zengxiaosen on 2017/5/23.
  */
object DataFrameUtils {
  def unionAll(dataFrames: Seq[DataFrame], sQLContext: SQLContext): DataFrame = dataFrames match {
    case Nil => sQLContext.emptyDataFrame
    case head :: Nil => head
    case head :: tail => head.unionAll(unionAll(tail, sQLContext))
  }
}
