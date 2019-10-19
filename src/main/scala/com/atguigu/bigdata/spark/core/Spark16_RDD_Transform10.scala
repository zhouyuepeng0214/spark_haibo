package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark16_RDD_Transform10 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    // 自定义分区器，将所有的数据放置在1号分区
    // 使用分区器
    val rdd1: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    val rdd2: RDD[(Int, (Int, String))] = rdd1.mapPartitionsWithIndex((index,datas) => {datas.map(x => (index,x))})

    rdd2.collect().foreach(println)

    // 释放资源
    sc.stop()

  }
}

// 声明分区器
// 继承Partitioner
// 重写方法numPartitions， getPartition
class MyPartitioner(num : Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = 1
}
