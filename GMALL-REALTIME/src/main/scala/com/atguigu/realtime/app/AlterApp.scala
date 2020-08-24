package com.atguigu.realtime.app

import com.atguigu.realtime.bean.{AlterInfo, EventLog}
import com.atguigu.realtime.util.{ESUtil, MyKafkaUtil}
import com.atguigu.util.Constant
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.JsonDSL._
import java._


import scala.util.control.Breaks._

object AlterApp extends BaseApp {

  implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

  override def doSomething(streamingContext: StreamingContext): Unit = {
    val sourceStream: DStream[String] = MyKafkaUtil.
      getKafkaStreaming(streamingContext, Constant.EVENT_LOG_TOPIC).
      window(Minutes(5), Seconds(6))
    // json4s

    val eventLogStream: DStream[EventLog] = sourceStream.map(json => JsonMethods.parse(json).extract[EventLog])
    // val eventLogStream: DStream[EventLog] = sourceStream.map(json => JSON.parseObject(json, classOf[EventLog]))

    // 1、按照mid进行分组
    val eventLogGroupedStream: DStream[(String, Iterable[EventLog])] = eventLogStream.
      map(log => log.mid -> log)
      .groupByKey

    // 2、计算优惠劵的领取情况
    val alterInfoStream: DStream[(Boolean, AlterInfo)] = eventLogGroupedStream.map {
      case (mid, logIt) => {
        // ① 保存领取优惠券的那些uid
        val uids: util.HashSet[String] = new util.HashSet[String]()
        // ② 保存优劵对用的商品id
        val items: util.HashSet[String] = new util.HashSet[String]()
        // ③ 保存5分钟内所有的事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()
        // ④ 记录5分钟内是否有点击商品的行为
        var isClicked: Boolean = false
        breakable {
          logIt.foreach(event => {
            events.add(event.eventId)
            event.eventId match {
              case "coupon" =>
                uids.add(event.uid)
                items.add(event.itemId)
              case "chickItem" =>
                isClicked = true
                break
              case _ => // 其他事件不作操作
            }
          })
        }
        // (是否预警，预警信息)
        (uids.size() >= 3 && !isClicked, AlterInfo(mid, uids, items, events, System.currentTimeMillis()))
      }
    }
    alterInfoStream.cache()
    alterInfoStream.print(1000)
    //3、把预警信息写入到es中
    alterInfoStream.filter(_._1).map(_._2).foreachRDD(rdd => {
      // 完成写入到es中
      import ESUtil._
      rdd.saveToES("gmall_coupon_alert")
    })
  }

  override var appName: String = "AlterApp"
}
