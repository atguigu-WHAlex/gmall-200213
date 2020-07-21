package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.OrderInfo
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.消费Kafka Order_info主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.ORDER_INFO), ssc)

    //3.将每一条数据转换为样例类对象
    val noDateorderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo
    })

    //4.数据的脱敏,处理时间
    val orderInfoDStream: DStream[OrderInfo] = noDateorderInfoDStream.map(orderInfo => {

      //a.取出创建时间 yyyy-MM-dd HH:mm:ss
      val create_time: String = orderInfo.create_time

      //b.将时间按照空格切割
      val dateTimeArr: Array[String] = create_time.split(" ")

      //c.赋值日期和小时
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)

      //d.脱敏手机号 ：13412349876
      //(134,12349876)
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
      //(1234,9876)
      val lastTelTuple: (String, String) = telTuple._2.splitAt(4)
      orderInfo.consignee_tel = s"${telTuple._1}****${lastTelTuple._2}"

      //e.返回
      orderInfo
    })

    orderInfoDStream.cache()

    //5.写入Phoenix
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL200213_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //测试打印
    orderInfoDStream.print()

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }


}
