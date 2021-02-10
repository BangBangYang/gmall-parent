package com.bupt.gmall2020.realtime.dim

import com.alibaba.fastjson.JSON
import com.bupt.gmall2020.realtime.bean.dim.ProvinceInfo
import com.bupt.gmall2020.realtime.util.{MyKafkaUtil, OffsetManger}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
/**
 * @author yangkun
 * @date 2021/2/10 19:37
 * @version 1.0
 */
object ProvinceApp {
  def main(args: Array[String]): Unit = {
    // 读取kafka中的省市的topic
    //加载流
    val sparkConf: SparkConf = new SparkConf().setAppName("province_app").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
//    val topic = "ODS_BASE_PROVINCE"
    val topic = "a"
    val groupId = "province_group"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManger.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver? executor?  //周期性的执行
      rdd
    }
    inputGetOffsetDstream.map(_.value()).print(100)
    // 写入到hbase中
    inputGetOffsetDstream.foreachRDD { rdd =>
      val provinceRDD: RDD[ProvinceInfo] = rdd.map { record =>
        val jsonString: String = record.value()
        val orderInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])
        orderInfo
      }
//      println(provinceRDD.take(10).toString)
      provinceRDD.saveToPhoenix("gmall2020_province_info",Seq("ID" , "NAME" , "AREA_CODE","ISO_CODE","ISO_3166_2")
        ,new Configuration
        ,Some("hdp4.buptnsrc.com:2181"))

      OffsetManger.setOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
