package com.atguigu.bigdata.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark38_RDD_Hbase1 {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val configuration: Configuration = HBaseConfiguration.create()
    //conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")

    // 向Hbase中写入数据
    val rdd: RDD[(String, String)] = sc.makeRDD(List( ("3001", "aaaa"), ("3002", "bbbb" ), ("3003", "cccc") ))

    // Put Scan Get
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.map {
      case (id, name) => {

        val rowkey = Bytes.toBytes(id)

        val put = new Put(rowkey)
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(rowkey), put)
      }
    }

    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")


    putRDD.saveAsHadoopDataset(jobConf)

    // 释放资源
    sc.stop()

  }
}
