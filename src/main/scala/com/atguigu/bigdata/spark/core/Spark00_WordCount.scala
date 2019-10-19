package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark00_WordCount {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),
      2)

    //todo 方法1
    val rdd5: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val rdd51: RDD[(String, Int)] = rdd5.map {datas => (datas._1,datas._2.sum)
//      case (w, t) => (w, t.sum)
    }
    //rdd51.collect().foreach(println)

    // todo 方法2
    //todo reduceByKey可以聚合数据，分区内和分区间的计算逻辑完全相同
    val rdd4: RDD[(String, Int)] = rdd.reduceByKey((x,y)=>x+y)
    //rdd4.collect().foreach(println)

    // todo 方法3
    val rdd3: RDD[(String, Int)] = rdd.aggregateByKey(0)((x,y)=>x+y,(x,y)=>x+y)
    //rdd3.collect()foreach(println)

    // todo 方法4
    val rdd2: RDD[(String, Int)] = rdd.foldByKey(0)((x,y)=>x+y)
    //rdd2.collect()foreach(println)

    // todo 方法5
    val rdd1: RDD[(String, Int)] = rdd.combineByKey[Int](
      (num: Int) => num,
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x + y
    )
    //rdd1.collect().foreach(println)

    // todo 方法6
    val rdd6: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(t => t._1)

    val rdd66: RDD[(String, Int)] = rdd6.map(datas => {
      (datas._1, datas._2.map(t => t._2))
    }).map {
      datas => (datas._1, datas._2.sum)
    }
    rdd66.collect().foreach(println)

    // todo 方法7  ("a",88) => 88 * （"a",1）????
    val rdd7: collection.Map[String, Long] = rdd.map(t => t._1*t._2).flatMap(
      word => word.split("")).map(t => (t,1)).countByKey()

    //rdd7.foreach(println)

    // todo 方法8
    val rdd8: collection.Map[String, Long] = rdd.map(t => t._1*t._2).flatMap(word => word.split("")).countByValue()
    //rdd8.foreach(println)

    //释放资源
    sc.stop()

  }

}
