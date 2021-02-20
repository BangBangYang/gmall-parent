package com.bupt.gmall2020.realtime.dim

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.bupt.gmall2020.realtime.bean.dim.UserInfo
import com.bupt.gmall2020.realtime.util.{MyKafkaUtil, OffsetManger}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @author yangkun
 * @date 2021/2/16 10:51
 * @version 1.0
 */
object UserInfoApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_user_info_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_USER_INFO";
    val groupId = "gmall_user_info_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManger.getOffset(topic,groupId)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
      //startInputDstream.map(_.value).print(1000)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val userInfoDstream: DStream[UserInfo] = inputGetOffsetDstream.map { record =>
      val userInfoJsonStr: String = record.value()
      val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
      val formattor = new SimpleDateFormat("yyyy-MM-dd")
      val date: util.Date = formattor.parse(userInfo.birthday)
      val curTs: Long = System.currentTimeMillis()
      val  betweenMs= curTs-date.getTime
      val age=betweenMs/1000L/60L/60L/24L/365L
      if(age<20){
        userInfo.age_group="20岁及以下"
      }else if(age>30){
        userInfo.age_group="30岁以上"
      }else{
        userInfo.age_group="21岁到30岁"
      }
      if(userInfo.gender=="M"){
        userInfo.gender_name="男"
      }else{
        userInfo.gender_name="女"
      }
      userInfo
    }

    userInfoDstream.foreachRDD{rdd=>
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("gmall2020_user_info",Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER","AGE_GROUP","GENDER_NAME")
        ,new Configuration,Some("hdp4.buptnsrc.com:2181"))

      OffsetManger.setOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }



}
