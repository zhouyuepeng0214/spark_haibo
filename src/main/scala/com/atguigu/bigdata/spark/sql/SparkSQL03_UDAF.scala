package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL03_UDAF {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL01_DF").setMaster("local[*]")

    // 创建Spark SQL环境对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //用户自定义UDAF

    val ageAvg = new MyAgeAvgFunction

    spark.udf.register("ageAvg",ageAvg)

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",20)))

    val df: DataFrame = rdd.toDF("id","name","age")

    df.createOrReplaceTempView("user")

    spark.sql("select ageAvg(age) from user").show()


    spark.stop()

  }

}

// todo 自定义聚合函数(弱类型)
// 1. 继承UserDefinedAggregateFunction
// 2. 重写方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction {

  // 输入数据的结构类型
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  // 缓冲区数据的结构类型
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  // 函数的返回结果类型
  override def dataType: DataType = {
    DoubleType
  }

  // 函数稳定性
  override def deterministic: Boolean = {
    true
  }

  // 缓冲区数据初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  // 合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
