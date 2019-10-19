package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator}
import org.apache.spark.sql._

object SparkSQL04_UDAF_Class {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL01_DF").setMaster("local[*]")

    // 创建Spark SQL环境对象
    //val spark = new SparkSession(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //用户自定义UDAF
    // 创建聚合函数
    val function = new MyAgeAvgClassFunction

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",20)))

    // todo 使用DSL语法来访问强类型聚合函数
    // 将聚合函数转换为查询的列

    val col: TypedColumn[EmpX, Double] = function.toColumn.name("avgAge")

    val rdd1: RDD[EmpX] = rdd.map {
      case (id, name, age) => {
        EmpX(id, name, age)
      }
    }
    val ds: Dataset[EmpX] = rdd1.toDS()

    ds.select(col).show()


    spark.stop()

  }

}

case class EmpX(id:Long,name:String,age:Long)
case class AvgBuffer(var sum:Long,var count:Long)

// 自定义聚合函数(强类型)
// 1. 继承Aggregator,并声明泛型
// 2. 重写方法

class MyAgeAvgClassFunction extends Aggregator[EmpX,AvgBuffer,Double] {
  //缓冲区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L,0L)
  }

  override def reduce(buffer: AvgBuffer, emp: EmpX): AvgBuffer = {
    buffer.sum = buffer.sum + emp.age
    buffer.count = buffer.count + 1L
    buffer
  }

  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}