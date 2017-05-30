package vr.func

/**
  * Created by zengxiaosen on 2017/5/30.
  */
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import utils.RedisUtils
import vr.entity.Video

import scala.collection.JavaConversions
object VideoCluster {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Video Cluster")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jedis = RedisUtils.pool.getResource
    jedis.select(6)
    val keys = jedis.keys("*")

    val videoKeys = JavaConversions.asScalaSet(keys)
    var responses = scala.collection.mutable.Map[String, String]()
    videoKeys.foreach(videoKey => {
      val videoValue = jedis.get(videoKey)
      responses += videoKey -> videoValue
    })

    var wordSet: Set[String] = Set()
    val dataSet = responses.foreach(item => {
      val videoProfile = parse(item._2, useBigDecimalForDouble = false)
      implicit val formats = DefaultFormats
      val video = videoProfile.extract[Video]
      //利用标题、描述、标签所有信息
      //val content = ChineseHandle.getWordStr(video.video_title+video.video_describe+video.video_tag).split(" ")
      //根据标题、描述、标签提取关键字后的信息
      //val content = ChineseHandle.chineseKeyWordComputer(ChineseHandle.getWordStr(video.video_title+video.video_tag),ChineseHandle.getWordStr(video.video_title+video.video_tag+video.video_describe),10).split(" ")
      //只利用标签信息
      val content = video.video_tag.split(",")
      content.foreach(word => {
        if (word.trim != "") {
          wordSet += word.trim
        }
      })
    })

    val testData = wordSet.map(Array(_)).toSeq.map(Tuple1.apply)
    val testDataDF = sqlContext.createDataFrame(testData).toDF("text")

    val outFile = sc.textFile("data/wiki_seg.txt").mapPartitions(iter => {
      iter.map(line => {
        Tuple1.apply(line.split(" "))
      })
    })

    val outFileDF = sqlContext.createDataFrame(outFile).toDF("text")


    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(10)
      .setMinCount(0)
    val w2vModel: Word2VecModel = word2Vec.fit(outFileDF)
    w2vModel.write.overwrite.save("data/w2vmodel")
    val kmeansDataSet = w2vModel.transform(testDataDF).withColumnRenamed("result", "features")
    // kmeansDataSet.cache()
    val K = 5
    val kmeans = new KMeans().setK(K).setSeed(1L)
    val kmeansModel = kmeans.fit(kmeansDataSet)
    val KMresult = kmeansModel.transform(kmeansDataSet)
    val i = 0
    for (i <- 0 to K) {
      KMresult.where(s"prediction = ${i}").select("prediction", "text").show(1000, false)
    }


  }
}
