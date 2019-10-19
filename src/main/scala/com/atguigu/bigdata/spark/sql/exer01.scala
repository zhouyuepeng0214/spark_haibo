package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object exer01 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("count").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val tbStockRDD: RDD[String] = spark.sparkContext.textFile("input/tbStock.txt")

    //tbStockRDD.foreach(println)

    val rdd1: RDD[String] = tbStockRDD.flatMap(_.split(","))

    val value: RDD[Array[String]] = tbStockRDD.map(_.split(","))




    spark.stop()

  }

}

case class tbStock(ordernumber:String,locationid : String,dateid : String) extends Serializable