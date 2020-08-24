package com.atguigu.canal

import java.net.{InetSocketAddress, SocketAddress}
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.atguigu.util.Constant
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._

object CanalClient {


  def sendToKafka(topic: String, message: String) = {
    MyKafkaUtil.send(topic, message)
  }

  /**
   * 处理RowData数据
   *
   * @param rowDatas
   * @param tableName
   * @param eventType
   * @return
   */
  def handleRowDatas(rowDatas: util.List[CanalEntry.RowData], tableName: String, eventType: CanalEntry.EventType) = {
    if(tableName == "order_info" && eventType == EventType.INSERT && rowDatas != null && !rowDatas.isEmpty) {
      for (rowData <- rowDatas.asScala) {
        val obj: JSONObject = new JSONObject()
        // 所有的列
        val columns: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
        for (column <- columns.asScala) {
          val name: String = column.getName
          val value: String = column.getValue
          obj.put(name, value)
        }
        // println(obj.toJSONString)
        sendToKafka(Constant.ORDER_INFO_TOPIC, obj.toJSONString)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val address = new InetSocketAddress("hadoop102", 11111)
    // 1、连接canal
    // 1.1 创建连接器对象
    val connector: CanalConnector = CanalConnectors.newSingleConnector(address, "example", "", "")
    // 1.2 连接到canal服务器
    connector.connect()
    // 2、拉取数据
    // 2.1 先订阅数据，订阅哪些数据哪些表
    connector.subscribe("gmall_realtime.*")

    while (true) {
      val message: Message = connector.get(100) // 最多拉取100条sql导致的变化的数据
      // 3、解析数据
      // 3.1 所有的数据均在这里；Entry : 表示一条sql导致的表
      val entries: util.List[CanalEntry.Entry] = message.getEntries
      if (entries != null && entries.size() > 0) {
        // 3.1 解析entry
        for(entry <- entries.asScala) {
          if (entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA) {
            val storeValue: ByteString = entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList

            handleRowDatas(rowDatas, entry.getHeader.getTableName, rowChange.getEventType)
          }

        }
      } else {
        println("没有拉取到数据，3s后重新拉取")
        Thread.sleep(3000)
      }
    }
  }
}
