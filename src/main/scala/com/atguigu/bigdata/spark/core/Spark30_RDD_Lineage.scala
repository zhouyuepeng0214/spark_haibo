package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark30_RDD_Lineage {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_+_)

    //println(rdd1.toDebugString)

    reduceRDD.dependencies.foreach(println)

    // 释放资源
    sc.stop()

  }
}
