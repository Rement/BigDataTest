package test
import org.apache.spark._
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object Main {
  def main(args: Array[String]) {
    //Initiate spark context with spark master URL. You can modify the URL per your environment. 
        var a = 0 
    val conf1 = new SparkConf().setAppName("WordCounter").setMaster("local[1]")
    val sc = new SparkContext(conf1)
    //val sparkConf = new SparkConf().setAppName("HBaseApp").setMaster("local[2]")
    //val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "webpage"

    //System.setProperty("user.name", "hdfs")
    //System.setProperty("HADOOP_USER_NAME", "hdfs")
    conf.set("hbase.master", "localhost:60000")
    conf.setInt("timeout", 100000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("Number of Records found : " + hBaseRDD.count())
    sc.stop()
  }
}