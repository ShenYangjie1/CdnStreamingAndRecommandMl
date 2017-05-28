package test.implict

/**
  * Created by zengxiaosen on 2017/5/28.
  */
object ImplicitClassDemo {

  implicit class MyImplicitTypeConversion(val str: String){
    def strToInt = str.toInt
  }


  def main(args: Array[String]): Unit = {
    import test.implict.ImplicitDefDemo.MyImplicitTypeConversion._
    val max = Math.max("1", 2)
    println(max)
  }

}
