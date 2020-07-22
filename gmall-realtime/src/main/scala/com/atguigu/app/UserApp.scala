package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.UserInfo
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka User主题数据创建流
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.USER_INFO), ssc)

    //3.将用户数据写入Redis
    userInfoKafkaDStream.map(_.value()).foreachRDD(rdd => {

      //每一个分区单独处理
      rdd.foreachPartition(iter => {
        //a.获取Redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.处理数据集iter
        iter.foreach(userInfoStr => {
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          jedisClient.set(s"user:${userInfo.id}", userInfoStr)
        })
        //c.归还连接
        jedisClient.close()
      })
    })

    //4.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
