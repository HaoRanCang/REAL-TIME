package com.atguigu.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait BaseApp {

  var appName : String
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(3))

    doSomething(ssc)

    // 启动流
    ssc.start()
    // 阻止进程退出
    ssc.awaitTermination()
  }

  def doSomething(streamingContext: StreamingContext)
}
