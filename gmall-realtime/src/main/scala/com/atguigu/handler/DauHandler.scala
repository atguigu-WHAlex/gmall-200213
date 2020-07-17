package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 去重1--->根据Redis做跨批次去重
    *
    * @param startUpLogDStream 从kafka读取的原始数据
    * @return
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], ssc: StreamingContext): DStream[StartUpLog] = {

    //方案一：单条过滤
    val value1: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {

      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //b.单条过滤(判断是否在Redis中已经存在)
      val boolean: lang.Boolean = !jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)

      //c.归还连接
      jedisClient.close()

      //d.将结果返回
      boolean
    })

    //方案二：一个分区获取一次Redis连接
    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
      //a.分区内获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.分区内过滤数据
      val logs: Iterator[StartUpLog] = iter.filter(startUpLog => !jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid))
      //c.归还连接
      jedisClient.close()
      //d.返回结果
      logs
    })

    //方案三：每个批次获取一次Redis连接
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {

      //一个批次调用一次,且在Driver端,在这个位置获取Redis中的所有Mids,广播至Executor
      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //b.使用当天时间
      val date: String = sdf.format(new Date(System.currentTimeMillis()))
      val uids: util.Set[String] = jedisClient.smembers(s"DAU:$date")

      //c.广播uids
      val uidsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uids)

      //d.归还连接
      jedisClient.close()

      //e.对RDD中的数据做去重
      rdd.filter(startUpLogDStream => !uidsBC.value.contains(startUpLogDStream.mid))
    })

    value1
    value2
    value3
  }


  /**
    * 将两次去重之后的Mid及日期写入Redis,提供给当天以后的批次做去重用
    *
    * @param startUpLogDStream 经过两次去重之后的数据集
    */
  def saveDateAndMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {

    //对各个分区单独写入数据
    startUpLogDStream.foreachRDD(rdd => {

      rdd.foreachPartition { iter =>

        //a.分区内获取一次Redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.对分区迭代,逐条写入
        iter.foreach(startUpLog => {
          //RedisKey  ->  DAU:2020-07-17  Set[String]  mids
          jedisClient.sadd(s"DAU:${startUpLog.logDate}", startUpLog.mid)
        })

        //c.归还连接
        jedisClient.close()
      }
    })

  }

}
