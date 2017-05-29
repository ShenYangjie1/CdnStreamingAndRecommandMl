package test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zengxiaosen on 2017/5/24.
  */
object test1 {
  def main(args: Array[String]) {
//    val ip = "1.2.3.43"
//    val temp = ip.split("\\.").reverse.zipWithIndex
//    temp.map(x=>{
//      println(x._1 + " " + x._2)
//    })
    var count = 0
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val a = sc.textFile("/d/receng/receng/README.md")
    a.foreach(x=>{
      println(x)
      count+=1
      println("count: "+ count)
    })

  }
}
