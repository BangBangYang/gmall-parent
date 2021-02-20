package com.bupt.gmall2020.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bupt.gmall2020.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManger}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @author yangkun
 * @date 2021/2/6 21:21
 * @version 1.0
 */
object BaseDBMaxwell {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("base_db_maxwell")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "gmall_DB_M"
    val groupId = "base_db_maxwell_group"
    var recordInputDStream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMap: Map[TopicPartition, Long] = OffsetManger.getOffset(topic, groupId)
    if (offsetMap != null && offsetMap.size > 0) {
      recordInputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordInputDStream = MyKafkaUtil.getKafkaStream(topic, ssc)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDStream.transform { rdd =>
      //      println(rdd.getClass.getSimpleName)
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver? executor?  //周期性的执行
      rdd
    }

    val jsonObjDStream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      //      println(record.getClass.getSimpleName) //  ConsumerRecord类型
      val recordString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(recordString)
      jsonObj
    }
    jsonObjDStream.foreachRDD { rdd =>
      //推送到kafka
      rdd.foreach { jsonObj =>
        if (jsonObj.getJSONObject("data") != null && !jsonObj.getJSONObject("data").isEmpty
          && !"delete".equals(jsonObj.getString("type")) &&
          (("order_info".equals(jsonObj.getString("table")) && "insert".equals(jsonObj.getString("type")))
            || "order_detail".equals(jsonObj.getString("table"))
            || "base_province".equals(jsonObj.getString("table"))
            || "user_info".equals(jsonObj.getString("table"))
            || "base_category3".equals(jsonObj.getString("table"))
            || "base_trademark".equals(jsonObj.getString("table"))
            || "spu_info".equals(jsonObj.getString("table"))
            || "sku_info".equals(jsonObj.getString("table")))) {
          val tableName: String = jsonObj.getString("table")
          val jsonString: String = jsonObj.getString("data")
          val topic = "ODS_" + tableName.toUpperCase
          println(jsonString)
          if (topic.equals("ODS_ORDER_INFO")) {
            //            Thread.sleep(100)
            MyKafkaSink.send(topic, jsonString) //非幂等的操作 可能会导致数据重复
          } else if (topic.equals("ODS_BASE_PROVINCE")) {

            MyKafkaSink.send("a", jsonString)
          } else if (topic.equals("ODS_ORDER_DETAIL")) {
            //            Thread.sleep(100)
            MyKafkaSink.send(topic, jsonString)
          } else if (topic.equals("ODS_USER_INFO")) {
            MyKafkaSink.send(topic, jsonString)
          } else if (topic.equals("ODS_BASE_CATEGORY3")) {
            MyKafkaSink.send(topic, jsonString)
          } else if (topic.equals("ODS_SPU_INFO")) {
            MyKafkaSink.send(topic, jsonString)
          } else if (topic.equals("ODS_BASE_TRADEMARK")) {
            MyKafkaSink.send(topic, jsonString)

          }else if (topic.equals("ODS_SKU_INFO")) {
            MyKafkaSink.send(topic, jsonString)
          }
        }
      }
      OffsetManger.setOffset(topic, groupId, offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()
  }


}
