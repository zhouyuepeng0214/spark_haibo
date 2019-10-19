package com.atguigu.bigdata.spark.core

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark35_RDD_Mysql {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/exer"
    val userName = "root"
    val passWd = "123456"

    val jdbc = new JdbcRDD(
      sc,
      ()=>{
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select * from rdd where id >= ? and id <= ?",
      1,
      3,
      3,
      (rs)=>{
        println(rs.getInt(1) + "," + rs.getString(2) + "," + rs.getInt(3))
      }
    )

    jdbc.collect()


    // 释放资源
    sc.stop()

  }
}
