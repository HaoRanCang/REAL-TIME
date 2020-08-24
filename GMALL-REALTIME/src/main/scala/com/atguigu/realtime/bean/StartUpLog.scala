package com.atguigu.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class StartUpLog(
                       mid: String,
                       uid: String,
                       appId: String,
                       area: String,
                       os: String,
                       channel: String,
                       logType: String,
                       version: String,
                       ts: Long,
                       var logDate: String = null, // 2020-08-18
                       var logHour: String = null // 10
                     ){
  private val date: Date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}
