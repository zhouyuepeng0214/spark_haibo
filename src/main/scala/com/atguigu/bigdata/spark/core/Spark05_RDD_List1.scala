package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_List1 {

    def main(args: Array[String]): Unit = {

        // 使用Spark框架完成第一个案例：WordCount

        // 准备Spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 获取Spark上下文环境对象 :
        val sc = new SparkContext(conf)

        // 从集合(内存)中创建RDD
        // 方法的第二个参数表示执行并行度（分区数）
        //val value: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7),4)

        // 保存数据
       rdd.saveAsTextFile("output")

        // 释放资源
        sc.stop()

    }
}
