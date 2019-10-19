package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_List {

    def main(args: Array[String]): Unit = {


        // 准备Spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 获取Spark上下文环境对象 :
        val sc = new SparkContext(conf)

        // 从集合(内存)中创建RDD
        // 并行
        //val value: RDD[Int] = sc.parallelize(List(1,2,3,4))
        // 生成RDD
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        val rdd2: RDD[Int] = rdd.map(_*2)
        rdd2.collect().foreach(println)

        //val value: RDD[List[Int]] = sc.makeRDD(Array(List(1,2), List(3,4), List(5,6)))

        // 释放资源
        sc.stop()

    }
}
