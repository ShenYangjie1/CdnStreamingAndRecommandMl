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

case class Video(video_title: String, video_url: String, video_duration: Long, video_upload_time: Long, video_describe: String, video_thumbnail_url: String, video_play_count: Long,
                 video_size: Long, video_width: Long, video_height: Long, v_r_format: String, ads_poster_url: String, ads_poster_describe: String, video_type: String,
                 video_tag: String, author_id: String, video_category_id: String, original_video: String, like_count: Long, collect_count: Long, summary: String, vertical_thumb: String, code: Long)

case class NewVideo(id:Long,videoTitle: String, videoUrl: String, videoDuration: Long, videoUploadTime: Long, videoDescribe: String, videoThumbnailUrl: String, videoPlayCount: Long,
                    videoSize: Long, videoHeight: Long, videoWidth: Long, vRFormat: String, adsPosterUrl: String, adsPosterDescribe: String, videoType: String,
                    videoTag: String, videoCategoryId: String,authorId: String,  originalVideo: String, likeCount: Long, collectCount: Long, summary: String, verticalThumb: String, code: Long, doubanId:String)

//object User{
//  def apply(id: String,
//  email: String,
//  phone: String,
//  name: String,
//  sex: String,
//  avatar_url: String,
//  time_stamp: String,
//  auth_id: String,
//  introduce: String,
//  birthday: String,
//  last_read_sys_msg_time: String,
//  last_read_privet_msg_time: String,
//  user_type: String
//  ): User = new User(id,
//  email,  phone,  name,  sex,  avatar_url,  time_stamp,  auth_id,
//  introduce,  birthday,  last_read_sys_msg_time,  last_read_privet_msg_time,  user_type
//  )
//}

//object UserSchema {
//  val f1 = StructField("id", IntegerType, nullable = true)
//  val f2 = StructField("email", StringType, nullable = true)
//  val f3 = StructField("phone", StringType, nullable = true)
//  val f4 = StructField("name", StringType, nullable = true)
//  val f5 = StructField("sex", StringType, nullable = true)
//  val f6 = StructField("avatar_url", StringType, nullable = true)
//  val f7 = StructField("time_stamp", IntegerType, nullable = true)
//  val f8 = StructField("auth_id", IntegerType, nullable = true)
//  val f9 = StructField("introduce", StringType, nullable = true)
//  val f10 = StructField("birthday", StringType, nullable = true)
//  val f11 = StructField("last_read_sys_msg_time", IntegerType, nullable = true)
//  val f12 = StructField("last_read_privet_msg_time", IntegerType, nullable = true)
//  val f13 = StructField("user_type", IntegerType, nullable = true)
//
//  def getSchema: StructType = {
//    val array = Array[StructField](f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13)
//    val schema = DataTypes.createStructType(array)
//    schema
//  }
//}
