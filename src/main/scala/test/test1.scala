package test

/**
  * Created by zengxiaosen on 2017/5/24.
  */
object test1 {
  def main(args: Array[String]): Unit = {
    val ip = "1.2.3.43"
    val temp = ip.split("\\.").reverse.zipWithIndex
    temp.map(x=>{
      println(x._1 + " " + x._2)
    })
  }
}
