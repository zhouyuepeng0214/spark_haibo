package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark32_RDD_Checkpoint {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    // todo Spark 检查点一般存储在HDFS上
    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(Array("atguigu"))

    val rdd1: RDD[String] = rdd.map(_+System.currentTimeMillis())

    // todo Spark 检查点可以切断血缘关系
    rdd1.checkpoint

    val rdd2: RDD[(String, Int)] = rdd1.map((_,1)).reduceByKey(_+_)

//    rdd.collect().foreach(println)
//    rdd1.collect().foreach(println)
//    rdd1.collect().foreach(println)
//    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
    println(rdd2.toDebugString)

    // 释放资源
    sc.stop()

  }
}
