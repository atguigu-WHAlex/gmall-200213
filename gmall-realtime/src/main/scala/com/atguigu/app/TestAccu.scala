package com.atguigu.app

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object TestAccu {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5))

    //4.定义累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    //5.将RDD数据扩大到2倍
    val rdd1: RDD[Int] = rdd.map(x => {
      sum.add(x)
      x * 2
    })

    //6.行动操作
    rdd1.collect().foreach(println)
    rdd1.collect().foreach(println)

    //7.打印累加器的值
    println(sum.value)

    //关闭连接
    sc.stop()

  }

}
