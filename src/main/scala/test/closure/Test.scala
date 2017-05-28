package test.closure

/**
  * Created by zengxiaosen on 2017/5/28.
  */
object Test {
  def main(args: Array[String]): Unit = {
    println( "muliplier(1) value = " +  multiplier(1) )
    println( "muliplier(2) value = " +  multiplier(2) )
  }

  var factor = 3
  val multiplier = (i: Int) => i * factor
}
