package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark29_RDD_Serializable {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    // 序列化
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    val search = new Search("h")

    //val matchRDD: RDD[String] = search.getMatch1(rdd)
    //rdd.filter(search.isMatch)

    // todo spark在执行作业之前，会先进行闭包检测，目的在于对闭包所使用的变量是否序列化进行检测
    val matchRDD: RDD[String] = search.getMatch2(rdd)

    matchRDD.foreach(println)

    // 释放资源
    sc.stop()

  }
}
class Search(query:String) extends Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    val q = query
    rdd.filter(x => x.contains(query))
  }

}
