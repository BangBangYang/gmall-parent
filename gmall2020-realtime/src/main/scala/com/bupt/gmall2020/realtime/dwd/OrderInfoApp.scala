package com.bupt.gmall2020.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bupt.gmall2020.realtime.bean.{OrderInfo, UserState}
import com.bupt.gmall2020.realtime.util.{MyKafkaUtil, OffsetManger, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2021/2/8 22:43
 * @version 1.0
 */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("order_info").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    val topic = "ODS_ORDER_INFO"
    val groupid = "ods_order_info_consumer"
    val offsetMap: Map[TopicPartition, Long] = OffsetManger.getOffset(topic,groupid)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size > 0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupid)
    }
    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream = recordInputStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver? executor?  //周期性的执行
      rdd
    }
    //基本的结构转换 ，补时间字段
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val recordString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(recordString,classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      val timeArr: Array[String] = createTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      orderInfo
    }
    //从hbase中获得用户是否为首单的状态
    val  orderInfoWithFirstFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderIter =>
      //每分区的操作
      val orderList: List[OrderInfo] = orderIter.toList
      if(orderList.size > 0){
        val userIds: List[Long] = orderList.map(_.user_id)
        val sql = "select user_id , if_consumed from user_state2020 where user_id in('" + userIds.mkString("','") + "')"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userMap: Map[String, String] = userStateList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap
        for(orderInfo <- orderList) { //每条消费数据
          val user_id: Long = orderInfo.user_id
          val if_consumed: String = userMap.getOrElse(user_id.toString, null)
          if (if_consumed != null && if_consumed == "1") { //如果是消费用户  首单标志置为0
            orderInfo.if_first_order = "0";
          } else {
            orderInfo.if_first_order = "1";
          }
        }
      }


      orderList.toIterator
    }

    //未优化，每条数据都要查询以下收据库，性能开销大
//    val orderInfoWithFirstFlagDstream: DStream[OrderInfo] = orderInfoDstream.map { orderInfo =>
//      val sql = "select user_id , if_consumed from user_state2020 where user_id='" + orderInfo.user_id + "'"
//      val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
//      if (userStateList != null && userStateList.size > 0) {
//        val user: JSONObject = userStateList(0)
//        if (user.getString("IF_CONSUMED").equals("1")) {
//          orderInfo.if_first_order = "0";
//        } else {
//          orderInfo.if_first_order = "1";
//        }
//      } else {
//        orderInfo.if_first_order = "1";
//      }
//      orderInfo
//    }
    orderInfoWithFirstFlagDstream.print(1000)
    // 保存 用户状态--> 更新hbase 维护状态
    orderInfoWithFirstFlagDstream.foreachRDD{rdd=>
      //driver
      //Seq 中的字段顺序 和 rdd中对象的顺序一直
      // 把首单的订单 更新到用户状态中
      val newConsumedUserRDD: RDD[UserState] = rdd.filter(_.if_first_order=="1").map(orderInfo=> UserState(orderInfo.user_id.toString,"1" ))
     //import org.apache.phoenix.spark._ 该方法增加了rdd的saveToPhoenix方法
      newConsumedUserRDD.saveToPhoenix("USER_STATE2020",Seq("USER_ID","IF_CONSUMED"),
        new Configuration,Some("hdp4.buptnsrc.com:2181"))


      OffsetManger.setOffset(topic,groupid,offsetRanges)
    }

//

    ssc.start()
    ssc.awaitTermination()

  }

}
