package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Transform12 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b",2), ("a",3), ("b",4)))

    //todo reduceByKey可以聚合数据，分区内和分区间的计算逻辑完全相同
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey((x,y)=>x+y)

    rdd1.collect()foreach(println)

    // 释放资源
    sc.stop()

  }
}
