package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by zengxiaosen on 2017/5/17.
  */
object LocalDataUtils {
  def getDataFromLocalFile(sc : SparkContext): RDD[String] ={
    //val path = "file:///data_b/xiaoliu/access.log.201701181850"  //local file
    val path = "hdfs:///user/xiaoliu/access.log.201701181850"
    val filedata = sc.textFile(path)
    filedata
  }
}
