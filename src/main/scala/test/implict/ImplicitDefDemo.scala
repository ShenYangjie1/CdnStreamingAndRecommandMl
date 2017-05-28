package test.implict

/**
  * Created by zengxiaosen on 2017/5/28.
  */
object ImplicitDefDemo {

  object MyImplicitTypeConversion{
    implicit def strToInt(str: String) = str.toInt
  }

  def main(args: Array[String]): Unit = {
    import MyImplicitTypeConversion.strToInt
    val max = math.max("1", 2)
    println(max)
  }

}
