package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Transform14 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),
      2)
    //分区内第一次遇见key的时候，转换结构
    //v => (v,1)
    // combineByKey 需要传递三个参数
    //1.将第一个key出现的v转换结构
    //2.第二个参数表示分区内计算规则
    //2.第三个参数表示分区间计算规则
    val value: RDD[(String, (Int, Int))] = rdd.combineByKey[(Int, Int)](
      (num : Int) => (num, 1),
      (t : (Int,Int), num : Int) => {
      (t._1 + num, t._2 + 1)
      },
      (t1 : (Int,Int), t2 : (Int,Int)) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    })
    value.collect()foreach(println)
    val value1: RDD[(String, Int)] = value.map {
      case (key, t) => {
        (key, t._1/t._2)
      }
    }
    //value1.collect()foreach(println)

    //wordcount
    val value2: RDD[(String, Int)] = rdd.combineByKey[Int](
      (num: Int) => num,
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x + y
    )
    //value2.collect()foreach(println)

    // 释放资源
    sc.stop()

  }
}
