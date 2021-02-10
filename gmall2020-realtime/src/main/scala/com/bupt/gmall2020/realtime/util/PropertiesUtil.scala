package com.bupt.gmall2020.realtime.util

import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @author yangkun
 * @date 2021/2/1 14:24
 * @version 1.0
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {

   val prop =  load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
    println(classOf[StringDeserializer])
  }
  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}


