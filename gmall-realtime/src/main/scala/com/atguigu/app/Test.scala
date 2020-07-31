package com.atguigu.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("1", "2", "3", "4"), 2)

    //034   041   034   034   043|034
    println(rdd.aggregate("0")((x, y) => Math.max(x.length, y.length).toString, (a, b) => a + b))

    //00    00    11    00    11
    println(rdd.aggregate("")((x, y) => Math.min(x.length, y.length).toString, (a, b) => a + b))

    //关闭连接
    sc.stop()

  }

}
