package com.atguigu.realtime.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092, hadoop103:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "gmall-realtime",
    // 从kafka最新的位置开始消费
    "auto.offset.reset" -> "latest",
    // offset是否自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def getKafkaStreaming(ssc : StreamingContext, topic : String) = {
    KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams)
    ).map(_.value())
  }

}
