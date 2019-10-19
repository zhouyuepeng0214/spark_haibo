package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Transform3 {

    def main(args: Array[String]): Unit = {

        // 准备Spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 获取Spark上下文环境对象 :
        val sc = new SparkContext(conf)

        val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        val value: RDD[(Int, Int)] = numRDD.mapPartitionsWithIndex((index, datas) => {
          datas.map(data => (index, data))
          //datas.map((index,_))
        })
        value.collect().foreach(println)

        // 释放资源
        sc.stop()

    }
}
