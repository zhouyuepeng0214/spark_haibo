package com.atguigu.bigdata.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark39_RDD_Sum {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // var sum = 0
    // todo 当前的程序存在问题，因为Executor中的计算结果不会返回到Driver
    //todo 采用累加器的数据结构，通知Spark将结果返回到Driver
    // 使用Spark自带的累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(num => {
      //sum=sum+num
      // 使用累加器
      sum.add(num)
    } )

    println(sum.value)
    //println(rdd.reduce(_+_))
    //println(rdd.sum())

    // 释放资源
    sc.stop()

  }
}
