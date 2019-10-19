package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL02_Transform {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL02_Transform").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List( (1, "zhangsan", 30), (2, "lisi", 40) ))

    import spark.implicits._

    // TODO 将RDD转换为DataFrame
    // RDD转换为DF，DS时，需要增加隐式转换，需要引入spark环境对象的隐式转换规则

//    /*
    val df: DataFrame = rdd.toDF("id","name","age")

    //df.show()

    // 将DataFrame转换为RDD

    val rdd1: RDD[Row] = df.rdd

    rdd1.foreach(row => {
//      println(row.getInt(0))
      println(row.get(0))
    })
//    */

    // TODO 将DF转换为DS

    /*
    val df: DataFrame = rdd.toDF("id","name","age")

    val ds: Dataset[User] = df.as[User]

    ds.show()
    
    val df1: DataFrame = ds.toDF()
    df1.show()
    */

    // TODO 将RDD转换为DataSet
/*
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    //userDS.show()

    val rdd1: RDD[User] = userDS.rdd

    rdd1.foreach(println)

*/
    //关闭spark
    spark.stop()

  }

}
case class User (id : Int,name : String,age : Int)
