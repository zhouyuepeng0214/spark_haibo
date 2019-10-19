package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Transform4 {

    def main(args: Array[String]): Unit = {

        // 准备Spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 获取Spark上下文环境对象 :
        val sc = new SparkContext(conf)


//        val value: RDD[List[Int]] = sc.makeRDD(List(List(1,2),List(3,4),List(5,6)))

        /*
        //转换算子 -- flatmap
        val value1: RDD[Int] = value.flatMap(list => list)

        value1.collect().foreach(println)
        */

        /*
        //转换算子 -- glom : 分区
        //分区最大
        val value2: RDD[Int] = sc.makeRDD(List(1,4,3,2),2)
        val value3: RDD[Array[Int]] = value2.glom()

        value3.collect().foreach(datas =>{
            println("****")
            datas.foreach(println)
            println(datas.max)
        })
        */

        /*
        //groupby
        val value: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        val value1: RDD[(Int, Iterable[Int])] = value.groupBy(num => num % 2)

        value1.collect().foreach(println)
        */

      /*
      // filter
        val value: RDD[String] = sc.makeRDD(List("xiaoming","xiaojiang","xiaohe","dazhi"))

        val value1: RDD[String] = value.filter(s => s.contains("xiao"))

        value1.collect().foreach(println)
       */
        // 释放资源
        sc.stop()

    }
}
