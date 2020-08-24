package com.atguigu.realtime.app
import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.OrderInfo
import com.atguigu.realtime.util.MyKafkaUtil
import com.atguigu.util.Constant
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object OrderApp extends BaseApp {

  override def doSomething(streamingContext: StreamingContext): Unit = {
    val sourceStreaming: DStream[String] = MyKafkaUtil.getKafkaStreaming(streamingContext, Constant.ORDER_INFO_TOPIC)

    // sourceStreaming.print()

    val orderInfoStream: DStream[OrderInfo] = sourceStreaming.map(json => JSON.parseObject(json, classOf[OrderInfo]))

    // 把数据写入到HBase中
    import org.apache.phoenix.spark._
    /*
    如果既要打印控制台又要进行rdd转换操作，则有可能导致数据丢失，解决办法是将kafka的数据进行缓存，或者在rdd转换内部打印rdd即可
    orderInfoStream.cache()
    orderInfoStream.print()
     */
    orderInfoStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT",
        "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME",
        "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO",
        "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), zkUrl = Some("hadoop102, hadoop103, hadoop104:2181"))
    })
  }

  override var appName: String = "OrderApp"
}
