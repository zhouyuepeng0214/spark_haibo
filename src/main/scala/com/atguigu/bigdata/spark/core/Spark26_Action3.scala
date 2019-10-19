package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark26_Action3 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2)),2)

    //rdd.saveAsTextFile("output")
    //rdd.saveAsObjectFile("output1")
    rdd1.saveAsSequenceFile("output1")



    // 释放资源
    sc.stop()

  }
}
