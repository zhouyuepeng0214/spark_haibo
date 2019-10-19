package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_DF {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL01_DF").setMaster("local[*]")

    // 创建Spark SQL环境对象
    //val spark = new SparkSession(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //创建DF
    val df: DataFrame = spark.read.json("input/user.json")

    df.createOrReplaceTempView("user")
    //df.createTempView("user")

    //spark.sql("select * from user" ).show

    df.select("name","age").show()

    spark.stop()

  }

}
