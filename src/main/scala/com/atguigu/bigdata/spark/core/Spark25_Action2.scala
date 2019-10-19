package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_Action2 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)

    //todo 初始值再分区内有效，在分区间计算也会使用
    val i: Int = rdd.aggregate(10)(_+_,_+_)  //51
    println(i)

    val j: Int = rdd.fold(10)(_+_)  //51
    println(j)


    // 释放资源
    sc.stop()

  }
}
