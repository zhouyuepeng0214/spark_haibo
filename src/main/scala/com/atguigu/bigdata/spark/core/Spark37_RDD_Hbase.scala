package com.atguigu.bigdata.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark37_RDD_Hbase {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)
    
    val configuration: Configuration = HBaseConfiguration.create()
    //configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104")
    configuration.set(TableInputFormat.INPUT_TABLE,"student")

    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    rdd.foreach{
      case (rowkey,result) => {
        for (cell <- result.rawCells()) {
          println(Bytes.toString(CellUtil.cloneRow(cell)) + "=" + Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }


    // 释放资源
    sc.stop()

  }
}
