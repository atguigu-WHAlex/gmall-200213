package com.atguigu.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {

  //读取配置文件
  private val properties: Properties = PropertiesUtil.load("config.properties")

  //创建消费Kafka的参数
  private val kafkaParams: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
    ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.group.id"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> properties.getProperty("kafka.deserializer"),
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> properties.getProperty("kafka.deserializer")
  )

  def getKafkaDStream(topics: Set[String], ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    //根据信息创建kafka数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics,
        kafkaParams))

    //返回Kafka数据流
    kafkaDStream
  }

}