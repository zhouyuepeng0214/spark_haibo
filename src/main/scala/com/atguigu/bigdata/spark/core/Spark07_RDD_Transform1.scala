package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Transform1 {

    def main(args: Array[String]): Unit = {

        // 使用Spark框架完成第一个案例：WordCount

        // 准备Spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 获取Spark上下文环境对象 :
        val sc = new SparkContext(conf)

        val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        //Driver Coding
        //所有算子的逻辑计算操作全部都是发送到Executor中执行的
        val newnumRDD: RDD[Int] = numRDD.map(
            //Executor
            {num => num * 2}
        )

        //newnumRDD.collect().foreach(println)
        newnumRDD.saveAsTextFile("output")

        // 释放资源
        sc.stop()

    }
}
