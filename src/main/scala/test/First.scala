package test

import driver.LocalProcess.process
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zengxiaosen on 2017/5/18.
  */
object First {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("DataTest")
      .setMaster("local")
//      .set("spark.default.parallelism", "108") //每个stage默认的task数量
//      .set("spark.storage.memoryFraction", "0.7") //RDD cache可以使用的内存 默认0.6
//      .set("spark.shuffle.file.buffer", "512k") //shuffle write的缓冲区大小
//      .set("spark.reducer.maxSizeInFlight", "256M") //shuffle read 的缓冲区大小
//      .set("spark.streaming.stopSparkContextByDefault", "true") //spark 任务优雅退出
//      .set("spark.shuffle.memoryFraction", "0.5") //shuffle可使用的内存，默认0.2

    val sc = new SparkContext(sparkConf)
    val a = sc.parallelize(List(1, 2, 3, 4))
    a.persist();
    println(a.count())
    println("============================")
    a.collect().foreach(println)
  }
}
