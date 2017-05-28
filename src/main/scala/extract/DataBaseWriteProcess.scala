package extract

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import prototype.StateStatusCalculate
import utils.{ConfigLoader, HBaseUtils}

/**
  * Created by zengxiaosen on 2017/5/23.
  */
object DataBaseWriteProcess {

  def hBaseWrite(rdd: RDD[Row]): Unit = {
    val sQLContext = SQLContext.getOrCreate(rdd.context)
    val unionDF = new StateStatusCalculate(rdd, sQLContext).outputDF()
    val namespaceTable = ConfigLoader.namespaceTable
    val userTable = TableName.valueOf(namespaceTable)
    val conf = HBaseUtils.createHBaseConf()
    val admin = new HBaseAdmin(conf)
    if (admin.tableExists(userTable)) {
    } else {
      HBaseUtils.createHTable(admin, namespaceTable)
    }
    HBaseUtils.writeBatchToHBase(unionDF, namespaceTable)
  }

}
