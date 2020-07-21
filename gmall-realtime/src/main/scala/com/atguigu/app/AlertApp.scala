package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka Event主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.GMALL_EVENT), ssc)

    //3.将每一行数据转换为样例类对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {

      //a.转换为样例类对象
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //b.处理日期
      val ts: Long = eventLog.ts
      val dateHour: String = sdf.format(new Date(ts))
      val dateHourArr: Array[String] = dateHour.split(" ")

      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      //c.返回结果
      eventLog
    })

    //4.开窗5min
    val eventLogAndWindowDStream: DStream[EventLog] = eventLogDStream.window(Minutes(5))

    //5.分组
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = eventLogAndWindowDStream.map(eventLog => (eventLog.mid, eventLog)).groupByKey()

    //6.三次及以上用不同账号登录并领取优惠劵：uids.size>=3
    //    并且过程中没有浏览商品:反面,只要有浏览商品,该数据不产生预警
    val boolToAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.map { case (mid, logIter) =>

      //定义Set用于存放领券的用户id
      val uids = new util.HashSet[String]()
      //定义一个Set用于存放领券涉及的商品ID
      val itemIds = new util.HashSet[String]()
      //定义集合用于存放发生过的行为
      val events = new util.ArrayList[String]()

      var noClick = true

      breakable(
        logIter.foreach(log => {

          //获取事件类型
          val evid: String = log.evid

          //将事件类型添加至集合
          events.add(evid)

          //如果为点击行为,则跳出当前循环
          if ("clickItem".equals(evid)) {
            noClick = false
            break()
            //如果为领券行为,则将用户ID以及商品ID添加至对应的集合
          } else if ("coupon".equals(evid)) {
            uids.add(log.uid)
            itemIds.add(log.itemid)
          }
        })
      )

      //生成疑似预警日志
      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }
    //过滤出待写入ES的预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToAlertInfoDStream.filter(_._1).map(_._2)

    //打印测试
    //    couponAlertInfoDStream.print()

    //7.写入ES
    val docIdToLogDStream: DStream[(String, CouponAlertInfo)] = couponAlertInfoDStream.map(log => {
      val dateHourMinu: String = sdf2.format(new Date(System.currentTimeMillis()))
      (s"${log.mid}_$dateHourMinu", log)
    })

    docIdToLogDStream.foreachRDD(rdd => {
      //按照分区写入
      rdd.foreachPartition(iter => {
        val date: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        MyEsUtil.insertBulk(s"${GmallConstants.ES_ALERT_INFO_PRE}_$date", iter.toList)
      })
    })

    //8.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}