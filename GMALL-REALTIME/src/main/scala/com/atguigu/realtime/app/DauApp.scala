package com.atguigu.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.StartUpLog
import com.atguigu.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.util.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(3))


    // 从kafka获取启动日志流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStreaming(ssc, Constant.START_LOG_TOPIC)
    // 解析启动日志，每条日志放入一个样例类
    val startupLogStream: DStream[StartUpLog] = sourceStream.map(jsonLog => JSON.parseObject(jsonLog, classOf[StartUpLog]))
    // 去重，保留每个设备的第一次起动记录
    /**
     * val filteredStartupLogStream: DStream[StartUpLog] = startupLogStream.filter(log => {
     * // 把mid写到redis的set，如果返回是0，保留，返回1，去掉
     * val client: Jedis = RedisUtil.getRedisClient
     * // key : "mid:2020-08-18" value:set(mid1,mid2,......)
     * val key = s"mid:${log.logDate}"
     * val result = client.sadd(key, log.mid)
     *client.close()
     * result == 1
     * })
     */

    // 一个分区建立一个到redis的连接
    val filteredStartupLogStream: DStream[StartUpLog] = startupLogStream.mapPartitions(startupLogIt => {
      val client: Jedis = RedisUtil.getRedisClient
      val finalResult: Iterator[StartUpLog] = startupLogIt.filter(log => {
        val key = s"mid:${log.logDate}"
        val result = client.sadd(key, log.mid)
        result == 1
      })
      client.close()
      finalResult
    })

    // filteredStartupLogStream.print(100)

    // 把数据写入到phoenix(HBase)
    import org.apache.phoenix.spark._

    filteredStartupLogStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    // 启动流
    ssc.start()
    // 阻止进程退出
    ssc.awaitTermination()
  }
}
