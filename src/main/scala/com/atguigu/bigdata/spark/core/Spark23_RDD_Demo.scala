package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Demo {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    // todo 需求：每个省份广告点击次数top3

    // todo 1 获取数据
    val lineRDD: RDD[String] = sc.textFile("input/agent.log")

    // todo 2 转换结构：（prv-adv，1）
    val mapRDD: RDD[(String, Int)] = lineRDD.map(line => {
      val datas: Array[String] = line.split(" ")
      (datas(1) + "-" + datas(4), 1)
    })

    // todo 3 转换结构：（prv-adv，1）=> (prv-adv，sum）
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    // todo 4 转换结构：(prv-adv，sum）=> (prv,(adv，sum)）
    val prvToAdv_SumRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("-")
        (keys(0), (keys(1), sum))
      }
    }

    // todo 5 转换结构：(prv,Iterator[(adv，sum)]）
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvToAdv_SumRDD.groupByKey()

    // todo 6 取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith((x, y) => (x._2 > y._2)).take(3)
    })

    // todo 7 打印
    resultRDD.collect().foreach(println)

    // 释放资源
    sc.stop()






  }
}
