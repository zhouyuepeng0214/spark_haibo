package com.atguigu.bigdata.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark36_RDD_Mysql1 {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/exer"
    val userName = "root"
    val passWd = "123456"

    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(List( (1, "zhangsan", 30), (2, "lisi", 20), (3, "wangwu", 40) ))

    /*
    rdd.foreach{
      case (id,name,age) => {
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url,userName,passWd)

        val sql = "insert into rdd(id,name,age) values(?,?,?)"

        val statement: PreparedStatement = connection.prepareStatement(sql)

        statement.setObject(1,id)
        statement.setObject(2,name)
        statement.setObject(3,age)

        statement.executeUpdate()
        statement.close()
        connection.close()

      }
    }
    */

    rdd.foreachPartition(datas=>{
      //Executor Coding
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = "insert into rdd ( id, name, age ) values (?, ?, ?)"
      val statement: PreparedStatement = connection.prepareStatement(sql)

      datas.foreach{
        case (id, name, age)=>{
          statement.setObject(1, id)
          statement.setObject(2, name)
          statement.setObject(3, age)
          statement.executeUpdate()
        }
      }

      statement.close()
      connection.close()
    })


    // 释放资源
    sc.stop()

  }
}
