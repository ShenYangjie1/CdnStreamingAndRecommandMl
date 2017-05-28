package utils

import java.math.BigInteger
import java.security.MessageDigest
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HConnection, HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by zengxiaosen on 2017/5/22.
  */
object HBaseUtils {
  def readTableData(admin: HBaseAdmin, namespaceTable: String) = {

  }


  val CF_FLOW: Array[Byte] = "flow".getBytes()
  val CF_BANDWIDTH: Array[Byte] = "bandwidth".getBytes
  val CF_LATENCY: Array[Byte] = "latency".getBytes
  val CF_REQUEST: Array[Byte] = "request".getBytes
  val CF_2XX: Array[Byte] = "2XX".getBytes
  val CF_3XX: Array[Byte] = "3XX".getBytes
  val CF_4XX: Array[Byte] = "4XX".getBytes
  val CF_5XX: Array[Byte] = "5XX".getBytes

  def getConn(conf: Configuration): HConnection = {
    HConnectionManager.createConnection(conf)
  }

  def createHTable(admin: HBaseAdmin, TABLENAME: String) {

    val userTable = TableName.valueOf(TABLENAME)
    //创建User表
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor(CF_FLOW))
    tableDescr.addFamily(new HColumnDescriptor(CF_BANDWIDTH))
    tableDescr.addFamily(new HColumnDescriptor(CF_LATENCY))
    tableDescr.addFamily(new HColumnDescriptor(CF_REQUEST))
    tableDescr.addFamily(new HColumnDescriptor(CF_2XX))
    tableDescr.addFamily(new HColumnDescriptor(CF_3XX))
    tableDescr.addFamily(new HColumnDescriptor(CF_4XX))
    tableDescr.addFamily(new HColumnDescriptor(CF_5XX))

    admin.createTable(tableDescr, getHexSplits("0", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16))
    println("Creating table  " + TABLENAME)
    admin.close()
  }

  private def getHexSplits(startKey: String, endKey: String, numRegions: Int): Array[Array[Byte]] = {
    val splits = new Array[Array[Byte]](numRegions - 1)
    var lowestKey = new BigInteger(startKey, 16)
    val highestKey = new BigInteger(endKey, 16)
    val range = highestKey.subtract(lowestKey)
    val regionIncrement = range.divide(BigInteger.valueOf(numRegions))
    lowestKey = lowestKey.add(regionIncrement)
    import java.math.BigInteger
    var i = 0
    while ( {
      i < numRegions - 1
    }) {
      val key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)))
      val b = String.format("%016x", key).getBytes
      splits(i) = b

      {
        i += 1
        i - 1
      }
    }
    splits
  }

  def createHBaseConf(): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf
  }

  def writeOneToHBase(rowkey: String, timesOfTheDay: Int, rawData: (Long, Long, Double, Long, Long, Long, Long, Long), sc: SparkContext, conf: Configuration, TABLENAME: String) : Unit = {
    // 写入 HBase
    val conn = getConn(conf)
    val table = conn.getTable(TableName.valueOf(TABLENAME))
    val putresult = convert(rowkey, timesOfTheDay, rawData)
    table.put(putresult)
    println("WRITE ONE SUCCESSFULLY!")
    table.close()
    conn.close()
  }

  def writeBatchToHBase(df: DataFrame, tableName: String): Unit = {
    // The repartition number had better be the same as the executor
    df.repartition(10).foreachPartition(iter => {
      val hbconf = createHBaseConf()
      val conn = getConn(hbconf)
      val table = conn.getTable(TableName.valueOf(tableName))
      val puts = new util.ArrayList[Put]()
      iter.foreach(record => {
        val flow = record.getAs[Long]("requestSize")
        val latency = record.getAs[Double]("requestTime")
        val bandwidth = ((flow * Constants.BYTELENTH) / Constants.STATSWIN).toLong
        val request = record.getAs[Long]("requestNum")
        val XX2 = record.getAs[Long]("xx2")
        val XX3 = record.getAs[Long]("xx3")
        val XX4 = record.getAs[Long]("xx4")
        val XX5 = record.getAs[Long]("xx5")
        val ts = record.getAs[Long]("eventTS")
        val domainCode = record.getAs[String]("domainCode")
        val stateCode = record.getAs[String]("stateCode")
        val rowkey = DateUtils.genDayTS(ts).toString + domainCode + stateCode
        val md5 = MessageDigest.getInstance("MD5")
        md5.update(rowkey.getBytes())
        val newRowkey = new BigInteger(1, md5.digest()).toString(16)
        val columnID = DateUtils.gen5MinsKey(ts)
        //(Long, Long, Float, Int, Int, Int, Int, Int)
        val row = (flow, bandwidth, latency, request, XX2, XX3, XX4, XX5)
        val put = batchConvert(newRowkey, columnID, row)
        puts.add(put)
      })
      table.put(puts)
      println("WRITE BATCH SUCCESSFULLY!")
      table.close()
      conn.close()
    })
  }

  def batchConvert(rowkey: String, timesOfTheDay: Int, row: (Long, Long, Double, Long, Long, Long, Long, Long)): Put = {
    val p = new Put(Bytes.toBytes(rowkey))
    p.add(CF_FLOW, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._1.toString))
    p.add(CF_BANDWIDTH, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._2.toString))
    p.add(CF_LATENCY, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._3.toString))
    p.add(CF_REQUEST, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._4.toString))
    p.add(CF_2XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._5.toString))
    p.add(CF_3XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._6.toString))
    p.add(CF_4XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._7.toString))
    p.add(CF_5XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._8.toString))
    p
  }

  def convert(rowkey: String, timesOfTheDay: Int, row: (Long, Long, Double, Long, Long, Long, Long, Long)): Put = {
    val p = new Put(Bytes.toBytes(rowkey))
    p.add(CF_FLOW, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._1.toString))
    p.add(CF_BANDWIDTH, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._2.toString))
    p.add(CF_LATENCY, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._3.toString))
    p.add(CF_REQUEST, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._4.toString))
    p.add(CF_2XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._5.toString))
    p.add(CF_3XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._6.toString))
    p.add(CF_4XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._7.toString))
    p.add(CF_5XX, Bytes.toBytes(timesOfTheDay.toString), Bytes.toBytes(row._8.toString))
    p

  }

}
