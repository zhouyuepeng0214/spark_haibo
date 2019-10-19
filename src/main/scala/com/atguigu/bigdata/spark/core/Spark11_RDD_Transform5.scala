package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Transform5 {

    def main(args: Array[String]): Unit = {

        // 使用Spark框架完成第一个案例：WordCount

        // 准备Spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 获取Spark上下文环境对象 :
        val sc = new SparkContext(conf)

        val numRDD: RDD[Int] = sc.makeRDD(1 to 10)

        //抽样
        //不放回：false，fraction：概率（0，1），seed： 随机数种子
        //放回：true，fraction：抽取次数（1，2），seed： 随机数种子
        val value: RDD[Int] = numRDD.sample(true,1)
        value.collect().foreach(println)

        // 释放资源
        sc.stop()

    }
}
