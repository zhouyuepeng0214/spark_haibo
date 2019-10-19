package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Transform11 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("b",2), ("a",3), ("b",4)))

    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    rdd1.collect()foreach(println)

    val rdd2: RDD[(String, Int)] = rdd1.map {
      case (c, datas) => (c, datas.sum)
    }
    rdd2.collect()foreach(println)


    // 释放资源
    sc.stop()

  }
}
