package com.atguigu.realtime.util

import com.atguigu.realtime.bean.AlterInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

object ESUtil {

  // 先得到ES的客户端
  val factory: JestClientFactory = new JestClientFactory
  val esURL = "http://hadoop102:9200"
  val config: HttpClientConfig = new HttpClientConfig.Builder(esURL)
    .maxTotalConnection(100)
    .connTimeout(1000*10)
    .readTimeout(1000*10)
    .build()
  factory.setHttpClientConfig(config)


  def main(args: Array[String]): Unit = {
    // insertSingle("user", User("zhangzhang", 10))
    // insertSingle("user", User("zhaozhao", 20), "10")
    insertBulk("user", List(
      ("11", User("a", 1)),
      ("22", User("b", 2)),
      ("33", User("c", 3))).toIterator)
  }

  def insertBulk(index : String, sources : Iterator[Object]) = {
    val client: JestClient = factory.getObject

    val bulkBuilder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(index)
        .defaultType("_doc")

    sources.foreach{
      case (id : String, source) => {
        val action: Index = new Index.Builder(source).id(id).build()
        bulkBuilder.addAction(action)
      }
      case source => {
        val action: Index = new Index.Builder(source).build()
        bulkBuilder.addAction(action)
      }
    }

    client.execute(bulkBuilder.build())
    // 关闭客户端
    client.shutdownClient()
  }

  def insertSingle(index : String, source : Object, id : String = null): Unit = {
    val client: JestClient = factory.getObject

    val action: Index = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()

    client.execute(action)

    // 关闭客户端
    client.shutdownClient()
  }

  implicit class RichRDD(rdd : RDD[AlterInfo]) {
    def saveToES(index : String): Unit = {
      rdd.foreachPartition{
        infoIt: Iterator[AlterInfo] => {
          ESUtil.insertBulk(index, infoIt.map(info => (s"${info.mid}:${System.currentTimeMillis()/1000/60}",info)))
        }
      }
    }
  }
}

case class User(name : String, age : Int)