import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zengxiaosen on 2017/5/18.
  */
object a {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("DataTest")


    val sc = new SparkContext(sparkConf)
    val a = sc.parallelize(List(1, 2, 3, 4))

    println(a.count())
    println("============================")
    a.collect().foreach(println)
  }
}
