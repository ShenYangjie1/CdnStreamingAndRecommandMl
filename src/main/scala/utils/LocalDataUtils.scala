package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by zengxiaosen on 2017/5/17.
  */
object LocalDataUtils {
  def getDataFromLocalFile(sc : SparkContext): RDD[String] ={
    //val path = "file:///data_b/xiaoliu/access.log.201701181850"  //local file
    val path = "/user/xiaoliu/access_log.txt"
    val filedata = sc.textFile(path)
    filedata.cache()
    println("zxs count: "+filedata.count())
    if (filedata == null)
      println("data is null")
    val filedataRdd = filedata.collect()
    filedataRdd.foreach(s =>{
      println("enter")
      println("row "+s)}
    )

    filedata
  }
}
