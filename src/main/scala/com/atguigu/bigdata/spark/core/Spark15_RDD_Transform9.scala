package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Transform9 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    //val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,1),(3,1),(4,1)))
    val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    //todo K_V类型的算子的源码都不在RDD中，通过隐式转换在PairRDDFunctions源码中查找
    //todo 转换算子 - partitionBy
    //todo 可以通过指定的分区器决定数据计算的分区,spark默认的分区器为HashPartitioner

    val rdd1: RDD[(Int, String)] = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))

    val rdd2: RDD[(Int, (Int, String))] = rdd1.mapPartitionsWithIndex((index,datas) => {datas.map((index,_))})

    rdd2.collect().foreach(println)


    // 释放资源
    sc.stop()

  }
}
