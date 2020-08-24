package com.atguigu.realtime.util

import redis.clients.jedis.Jedis

object RedisUtil {
  val host = "hadoop102"
  val port = 6379
  def getRedisClient = new Jedis(host, port)
}
