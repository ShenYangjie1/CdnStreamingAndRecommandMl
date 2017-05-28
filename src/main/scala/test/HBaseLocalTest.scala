package test
import javax.ws.rs.core.Response.Status.Family

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Result
/**
  * Created by zengxiaosen on 2017/5/22.
  */
object HBaseLocalTest {

  //创建表
  def createTable(connection: Connection, tablename: String): Unit = {
    //Hbase表模式管理器
    val admin = connection.getAdmin
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      // 创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      // 创建column family  artitle
      tableDescriptor.addFamily(new HColumnDescriptor("artitle".getBytes()))
      // 创建column family  author
      tableDescriptor.addFamily(new HColumnDescriptor("author".getBytes()))
      // 创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }


  }

  //删除表
  def deleteHTable(connection:Connection,tablename:String):Unit={
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //Hbase表模式管理器
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)){
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

  }
  //插入记录
  def insertHTable(connection:Connection,tablename:String,family:String,column:String,key:String,value:String):Unit= {
    try {
      val userTable = TableName.valueOf(tablename)
      val table = connection.getTable(userTable)
      //准备key的数据
      val p = new Put(key.getBytes)
      //为put操作指定column 和 value
      p.addColumn(family.getBytes, column.getBytes, value.getBytes())
      table.put(p)
    }
  }
  //基于key查询某条数据
  def getAResult(connection: Connection, tablename: String, family: String, column: String, key: String): Unit = {
    var table: Table = null
    try{
      val userTable = TableName.valueOf(tablename)
      table = connection.getTable(userTable)
      val g = new Get(key.getBytes())
      val result = table.get(g)
      val value = Bytes.toString(result.getValue(family.getBytes(),column.getBytes()))
      println("key: " + value)
    }finally {
      if(table!= null) table.close()
    }
  }


  def deleteRecord(connection:Connection,tablename:String,family:String,column:String,key:String): Unit ={
    var table:Table=null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val d=new Delete(key.getBytes())
      d.addColumn(family.getBytes(),column.getBytes())
      table.delete(d)
      println("delete record done.")
    }finally{
      if(table!=null)table.close()
    }
  }

  // 扫描记录
  def scanRecord(connection: Connection, tablename: String, family: String, column: String): Unit = {
    var table: Table = null
    var scanner: ResultScanner = null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val s = new Scan()
      s.addColumn(family.getBytes(), column.getBytes())
      val scanner = table.getScanner(s)
      println("scan...for...")
      var result:Result=scanner.next()
      while(result!=null) {
        println("Found row:" + result)
        println("Found value: " + Bytes.toString(result.getValue(family.getBytes(), column.getBytes())))
        result = scanner.next()
      }
    }finally {
      if(table != null){
        table.close()
        scanner.close()
      }
    }
  }



}
