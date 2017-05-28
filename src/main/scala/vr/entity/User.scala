package vr.entity

/**
  * Created by zengxiaosen on 2017/5/26.
  */
//[
//{
//"id": 888,
//"email": "",
//"phone": "15311415737",
//"name": "vr前",
//"sex": "m",
//"avatar_url": "20170412161804_b695810e.jpeg",
//"time_stamp": 1491985039105,
//"auth_id": 0,
//"introduce": " ",
//"birthday": "",
//"last_read_sys_msg_time": 1491985039575,
//"last_read_privet_msg_time": 0,
//"user_type": 0
//}
//]
case class User(email: String, phone: String, name: String, sex: String, avatar_url: String, area: String, mobileSimType: String,
                zipCode: String, time_stamp: String, auth_id: String, introduce: String, birthday: String, age: Int, zodica: String,
                constellation: String, last_read_sys_msg_time: Long, last_read_privet_msg_time: String, user_type: Long)

