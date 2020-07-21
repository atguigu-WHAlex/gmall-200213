package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.{OrderDetail, OrderInfo, UserInfo}
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka三个主题的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.ORDER_INFO), ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.ORDER_DETAIL), ssc)
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.USER_INFO), ssc)

    //3.转换为样例类
    val orderInfoDStream: DStream[OrderInfo] = orderInfoKafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
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

    val orderDetailDStream: DStream[OrderDetail] = orderDetailKafkaDStream.map(record => {
      JSON.parseObject(record.value(), classOf[OrderDetail])
    })

    val userInfoDStream: DStream[UserInfo] = userInfoKafkaDStream.map(record => {
      JSON.parseObject(record.value(), classOf[UserInfo])
    })

    //打印测试
    orderInfoDStream.print()
    //    orderDetailDStream.print()
    //    userInfoDStream.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
