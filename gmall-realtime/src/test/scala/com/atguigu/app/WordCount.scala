package com.atguigu.app

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //1.创建配置信息(SparkConf)
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.配置Kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata0213",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //4.读取Kafka数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("first"),
        kafkaParams))

    //5.处理数据,计算WordCount

    //Driver：全局一次
    println(s"${Thread.currentThread().getName}11111111111111111111")

    kafkaDStream.transform(rdd => {

      //Driver：每个批次一次
      println(s"${Thread.currentThread().getName}2222222222222222222222")

      rdd.map(_.value())
        .flatMap(_.split(" "))
        .map(x => {
          //Executor：每条数据一次
          println(s"${Thread.currentThread().getName}333333333333333333333333")
          (x, 1)
        })
        .reduceByKey(_ + _)

    })
      .print()

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
