package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

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

    //4.将OrderInfo和OrderDetail进行FULLJOIN
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val orderInToInfoAndDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //5.处理JOIN后的数据
    val noUserSaleDetailDStream: DStream[SaleDetail] = orderInToInfoAndDetailDStream.mapPartitions(iter => {

      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放JOIN上的数据
      val saleDetails = new ListBuffer[SaleDetail]
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //b.遍历iter,处理一条条的数据
      iter.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //定义RedisKey
        val orderKey = s"order:$orderId"
        val detailKey = s"detail:$orderId"

        //1.info数据不为空
        if (infoOpt.isDefined) {

          //取出info数据
          val orderInfo: OrderInfo = infoOpt.get

          //1.1 判断detail数据是否为空,如果不为空,结合写出
          if (detailOpt.isDefined) {
            //取出detail数据
            val orderDetail: OrderDetail = detailOpt.get
            //结合放入集合
            saleDetails += new SaleDetail(orderInfo, orderDetail)
          }

          //1.2 将info数据转换为JSON数据写入Redis
          //val orderInfoStr: String = JSON.toJSONString(orderInfo) 不行,编译报错
          val orderInfoStr: String = Serialization.write(orderInfo)
          jedisClient.set(orderKey, orderInfoStr)
          jedisClient.expire(orderKey, 120)

          //1.3 查询detail缓存,如果有数据,结合写出
          if (jedisClient.exists(detailKey)) {
            val detailStrSet: util.Set[String] = jedisClient.smembers(detailKey)
            import scala.collection.JavaConversions._
            detailStrSet.foreach(detailStr => {
              //将detailStr转换为样例类对象
              val orderDetail: OrderDetail = JSON.parseObject(detailStr, classOf[OrderDetail])
              //结合放入集合
              saleDetails += new SaleDetail(orderInfo, orderDetail)
            })
          }

          //2.info数据为空
        } else {

          //获取OrderDetail数据
          val orderDetail: OrderDetail = detailOpt.get

          //2.1 查询info缓存,如果有数据,结合写出
          if (jedisClient.exists(orderKey)) {
            //取出Redis中OrderInfo数据
            val orderInfoStr: String = jedisClient.get(orderKey)
            //转换orderInfoStr为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
            //结合放入集合
            saleDetails += new SaleDetail(orderInfo, orderDetail)
          } else {
            //2.2 查询info缓存,如果没有数据,则将detail数据转换为JSON格式写入Redis
            val orderDetailStr: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailKey, orderDetailStr)
            jedisClient.expire(detailKey, 120)
          }
        }
      }

      //c.归还连接
      jedisClient.close()

      //d.返回数据
      saleDetails.toIterator
    })

    //普通JOIN
    //    val value: DStream[SaleDetail] = orderIdToInfoDStream.join(orderIdToDetailDStream).map {
    //      case (_, (orderInfo, orderDetail)) =>
    //        new SaleDetail(orderInfo, orderDetail)
    //    }

    noUserSaleDetailDStream.print(100)

    //打印测试
    //    orderInfoDStream.print()
    //    orderDetailDStream.print()
    //    userInfoDStream.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}