package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Transform16 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((3,"a"),(4,"c"),(2,"b"),(1,"d")))

    //val rdd3: RDD[(Int, (String, String))] = rdd.join(rdd1)
    val rdd3: RDD[(Int, (Iterable[String], Iterable[String]))] = rdd.cogroup(rdd1)

    rdd3.collect()foreach(println)

    // 释放资源
    sc.stop()

  }
}
