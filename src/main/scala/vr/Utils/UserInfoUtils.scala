package vr.Utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.jsoup.Jsoup

/**
  * Created by zengxiaosen on 2017/5/29.
  */
object UserInfoUtils {

  /**
    * 根据手机号码计算相关信息
    *
    */
  def getMobileInfo(mobile: String): Array[String] = {
    var url = "http://www.ip138.com:8080/search.asp?action=mobile&mobile=%s"
    url = String.format(url, mobile)
    val doc = Jsoup.connect(url).get
    val els = doc.getElementsByClass("tdc2")
    val area = els.get(1).text
    val mobileSimType = els.get(2).text
    val mobileZone = els.get(3).text
    val zipCode = els.get(4).text.substring(0, 6)
    //    System.out.println("归属地：" + els.get(1).text)
    //    System.out.println("类型：" + els.get(2).text)
    //    System.out.println("区号：" + els.get(3).text)
    //    System.out.println("邮编：" + els.get(4).text.substring(0, 6))
    Array(area, mobileSimType, zipCode)
  }

  /**
    * 根据生日计算年龄
    *
    * @param birthday 生日
    */
  def getAgeFromBirth(birthday: String): Int = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse(birthday)
    getAge(date)
    //    println("年龄 " + getAge(date))
  }

  private def getAge(dateOfBirth: Date): Int = {
    var age = 0
    val born = Calendar.getInstance
    val now = Calendar.getInstance
    if (dateOfBirth != null) {
      now.setTime(new Date())
      born.setTime(dateOfBirth)
      if (born.after(now)) throw new IllegalArgumentException("年龄不能超过当前日期")
      age = now.get(Calendar.YEAR) - born.get(Calendar.YEAR)
      val nowDayOfYear = now.get(Calendar.DAY_OF_YEAR)
      val bornDayOfYear = born.get(Calendar.DAY_OF_YEAR)
      if (nowDayOfYear < bornDayOfYear) age -= 1
    }
    age
  }

  val zodiacArr = Array("猴", "鸡", "狗", "猪", "鼠", "牛", "虎", "兔", "龙", "蛇", "马", "羊")
  val constellationArr = Array("水瓶座", "双鱼座", "白羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天秤座", "天蝎座", "射手座", "魔羯座")
  val constellationEdgeDay = Array(20, 19, 21, 21, 21, 22, 23, 23, 23, 23, 22, 22)

  /**
    * 根据生日计算属相
    *
    * @param birthday 生日
    */
  def getZodica(birthday: String): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance
    val date = dateFormat.parse(birthday)
    cal.setTime(date)
    val zodia = zodiacArr(cal.get(Calendar.YEAR) % 12)
    //    println(zodia)
    zodia
  }

  //计算星座
  def getConstellation(birthday: String): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance
    val date = dateFormat.parse(birthday)
    cal.setTime(date)
    var month = cal.get(Calendar.MONTH)
    val day = cal.get(Calendar.DAY_OF_MONTH)
    if (day < constellationEdgeDay.apply(month)) {
      month = month - 1
    }
    if (month > 0) {
      val constellation = constellationArr(month)
      //      println(constellation)
      return constellationArr(month)
    }
    constellationArr(11)
  }


}
