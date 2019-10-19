package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File {

    def main(args: Array[String]): Unit = {

        // 使用Spark框架完成第一个案例：WordCount

        // 准备Spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 获取Spark上下文环境对象 :
        val sc = new SparkContext(conf)

        // 从存储系统中读取数据形成RDD
        // 存储系统路径默认为相对路径，相对当前的项目根路径
        // 读取文件的方式基于hadoop的读取方式
        // 读取文件的数据是一行一行的字符串
        val lineRDD: RDD[String] = sc.textFile("input")
        //val lineRDD: RDD[String] = sc.textFile("hdfs://hadoop102:9000/input/1.txt")
        lineRDD.collect()foreach(println)

        // 释放资源
        sc.stop()

    }
}
