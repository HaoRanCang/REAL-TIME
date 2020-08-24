package com.atguigu.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaUtil {
  // Kafka服务端的主机名和端口号
  private val properties: Properties = new Properties()
  properties.put("bootstrap.servers", "hadoop102:9092, hadoop103:9092, hadoop104:9092")
  // key序列化
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // value序列化
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

  def send(topic : String, content : String) = {
    producer.send(new ProducerRecord[String, String](topic, content))
  }
}
