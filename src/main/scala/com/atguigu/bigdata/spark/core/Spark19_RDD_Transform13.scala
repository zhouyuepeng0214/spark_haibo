package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Transform139 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("b",3),("a",3),("b",4),("a",5)),2)

    //aggregateByKey :使用了函数柯里化
    //存在两个参数列表
    //第一个参数表示分区内计算时的初始值(零值)
    //第二个参数列表中需要传递两个参数
    //  第一个参数表示分区内计算规则
    //  第二个参数表示分区间计算规则
    //val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(10)((x,y) => {Math.max(x,y)},(x,y) => {x + y})
    //val value: RDD[(String, Int)] = rdd.aggregateByKey(0)((x,y) => {x + y},(x , y) => {x + y})
    val value: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)

    //todo foldbykey 就是aggregateByKey的简化版，当aggregateByKey中分区内和分区间的计算规则一样时，
    //todo 使用foldByKey就可以了
    val value1: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
    value.collect()foreach(println)
    value1.collect().foreach(println)

    // 释放资源
    sc.stop()

  }
}
