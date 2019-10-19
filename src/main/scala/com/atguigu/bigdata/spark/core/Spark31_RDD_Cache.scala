package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark31_RDD_Cache {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

//    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))
//
//    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
//    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_+_)

    //println(rdd1.toDebugString)
    //reduceRDD.map(t=>t._1).collect
    //reduceRDD.map(t=>t._2).collect

    //reduceRDD.dependencies.foreach(println)

    val rdd = sc.makeRDD(Array("atguigu"))

    val cache = rdd.map(_.toString+System.currentTimeMillis)
//    val cache = rdd.map(_.toString+System.currentTimeMillis).cache()
    cache.collect();
    println(cache.toDebugString)

    // todo 缓存不能切断血缘关系，因为缓存数据可能丢失，丢失后需要从新根据血缘进行查找。

//    println(cache.collect.mkString(","))
//    println(cache.collect.mkString(","))
//    println(cache.collect.mkString(","))
//    println(cache.collect.mkString(","))

    // 释放资源
    sc.stop()

  }
}
