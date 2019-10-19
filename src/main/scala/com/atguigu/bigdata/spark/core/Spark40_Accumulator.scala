package com.atguigu.bigdata.spark.core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object Spark40_Accumulator {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello","Spark","hadoop","scala"))

    //创建累加器
    val accumulator = new BlackNameListAccumulator()

    //将累加器注册到spark中
    sc.register(accumulator,"blackNameList")

    rdd.foreach{
      word => {
        accumulator.add(word)
      }
    }

    println(accumulator.value)

    // 释放资源
    sc.stop()

  }
}

//自定义累加器
class BlackNameListAccumulator extends AccumulatorV2[String,java.util.HashSet[String]] {

  private val blackNameSet = new util.HashSet[String]()

  //当前累加器是否初始化
  override def isZero: Boolean = {
    blackNameSet.isEmpty
  }
  //复制累加器
  override def copy(): AccumulatorV2[String, util.HashSet[String]] = {
    new BlackNameListAccumulator
  }

  //重置累加器
  override def reset(): Unit = {
    blackNameSet.clear()
  }
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      blackNameSet.add(v)
    }
  }
  //合并
  override def merge(other: AccumulatorV2[String, util.HashSet[String]]): Unit = {
    blackNameSet.addAll(other.value)
  }

  //类型等同于累加器的输出类型
  override def value: util.HashSet[String] = {
    blackNameSet
  }
}

