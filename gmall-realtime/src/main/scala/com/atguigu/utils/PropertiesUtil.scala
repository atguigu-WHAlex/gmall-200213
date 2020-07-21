package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertieName: String): Properties = {

    //创建配置信息对象
    val prop = new Properties()

    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))

    //返回
    prop
  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = load("config.properties")
    val kafka_broker_list: String = properties.getProperty("redis.host")
    println(kafka_broker_list)
  }
}
