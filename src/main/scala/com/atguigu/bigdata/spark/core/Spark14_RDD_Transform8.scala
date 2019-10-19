package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Transform8 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    //union : 并集
    //substract : 差集
    //union : 交集
    //cartesian : 笛卡尔乘集
    //zip : 拉链
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7),2)
    //rdd1.union(rdd2).collect()foreach(println)
    //rdd2.subtract(rdd1).collect()foreach(println)
    //rdd2.intersection(rdd1).collect()foreach(println)
    //rdd1.cartesian(rdd2).collect()foreach(println)
    rdd1.zip(rdd2).collect()foreach(println)   //todo 分区数相同，分区内元素数量相同






    // 释放资源
    sc.stop()

  }
}
