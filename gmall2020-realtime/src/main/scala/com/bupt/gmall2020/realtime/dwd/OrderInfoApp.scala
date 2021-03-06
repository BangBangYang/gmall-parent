package com.bupt.gmall2020.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.bupt.gmall2020.realtime.bean.dim.{ProvinceInfo, UserState}
import com.bupt.gmall2020.realtime.bean.OrderInfo
import com.bupt.gmall2020.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManger, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast

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
    //1、从kafka中得到数据流
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size > 0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupid)
    }
    //2、得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream = recordInputStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver? executor?  //周期性的执行
      rdd
    }
    //3、预处理
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

    //4、从hbase中获得用户是否为首单的状态
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

    //4、从hbase中获得用户是否为首单的状态
    // 未优化，每条数据都要查询以下收据库，性能开销大
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
//    orderInfoWithFirstFlagDstream.print(1000)

    //6、修复如果新用户在同一批次 多次下单 会造成 该批次该用户所有订单都识别为首单
    // 利用hbase  进行查询过滤 识别首单，只能进行跨批次的判断
    //  如果新用户在同一批次 多次下单 会造成 该批次该用户所有订单都识别为首单
    //  应该同一批次一个用户只有最早的订单 为首单 其他的单据为非首单
    // 处理办法： 1 同一批次 同一用户  2 最早的订单  3 标记首单
    //           1 分组： 按用户      2  排序  取最早  3 如果最早的订单被标记为首单，除最早的单据一律改为非首单
    //           1  groupbykey       2  sortWith    3  if ...

    //6.1 调整结果变为k-v 为分组做准备
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDstream.map(orderInfo =>(orderInfo.user_id,orderInfo))
    //6.2 分组 按用户
    val orderInfoGroupByUidDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithKeyDstream.groupByKey()
    val orderInfoWithFirstRealFlagDstream: DStream[OrderInfo] = orderInfoGroupByUidDstream.flatMap { case (uid, orderInfoIter) =>
      //组内进行排序 ，最早的订单被标记为首单，除最早的单据一律改为非首单
      if (orderInfoIter.size > 1) {
        val orderInfoList: List[OrderInfo] = orderInfoIter.toList
        //组内进行排序
        val orderInfoSorted: List[OrderInfo] = orderInfoList.sortWith((orderInfo1, orderInfo2) => (orderInfo1.create_time < orderInfo2.create_time))
        val orderInfoFirst: OrderInfo = orderInfoSorted(0)
        if (orderInfoFirst.if_first_order == "1") {
          for (i <- 1 until orderInfoList.size) {
            val orderInfo: OrderInfo = orderInfoList(i)
            orderInfo.if_first_order = "0"
          }
        }
        orderInfoSorted
      } else {
        orderInfoIter.toList
      }

    }
    //7、合并关联省份维度信息
    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoWithFirstRealFlagDstream.transform { rdd =>

      //7.1 driver  按批次周期性执行，查询省市的信息
      //driver中查询 --------------------> 每个批次查询一次hbase，如果放在主代码块，那么只有在加载程序的时候才查询一次
      val sql = "select  id,name,area_code,iso_code,iso_3166_2 from gmall2020_province_info"
      val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
      val provinceMap: Map[String, ProvinceInfo] = provinceInfoList.map { jsonObj =>
        val provinceInfo = ProvinceInfo(
          jsonObj.getString("ID"),
          jsonObj.getString("NAME"),
          jsonObj.getString("AREA_CODE"),
          jsonObj.getString("ISO_CODE"),
          jsonObj.getString("ISO_3166_2")
        )
        (provinceInfo.id, provinceInfo)
      }.toMap
      //7.2使用广播变量将省市信息广播
      val provinceBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)
      //7.3将orderInfo确实的省市信息填充
      val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo => //在excutor执行
        val provinceMap: Map[String, ProvinceInfo] = provinceBC.value
        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id.toString, null)
        if (provinceInfo != null) {
          orderInfo.province_name = provinceInfo.name
          orderInfo.province_area_code = provinceInfo.area_code
          orderInfo.province_iso_code = provinceInfo.iso_code
          orderInfo.province_iso_3166_2 = provinceInfo.iso_3166_2
        }
        orderInfo
      }
      orderInfoWithProvinceRDD
    }
    //////////// 合并用户信息
    //////////////////用户信息关联//////////////////////////
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.mapPartitions { orderInfoItr =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      if(orderList.size>0) {
        val userIdList: List[Long] = orderList.map(_.user_id)
        val sql = "select id ,user_level ,  birthday  , gender  , age_group  , gender_name from gmall2020_user_info where id in ('" + userIdList.mkString("','") + "')"
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userJsonObjMap: Map[Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
        for (orderInfo <- orderList) {
          val userJsonObj: JSONObject = userJsonObjMap.getOrElse(orderInfo.user_id, null)
          orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
          orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
        }
      }
      orderList.toIterator
    }
    //8 orderInfoWithUserDstream 进行分流操作，最好cache一下避免重复计算
    //1.保存在hbse中
    //2.保存在es中
    orderInfoWithUserDstream.cache()

    // 8.1、保存 用户状态--> 更新hbase 维护状态
    orderInfoWithUserDstream.foreachRDD{rdd=>
      //driver
      //Seq 中的字段顺序 和 rdd中对象的顺序一直
      // 把首单的订单 更新到用户状态中
      val newConsumedUserRDD: RDD[UserState] = rdd.filter(_.if_first_order=="1").map(orderInfo=> UserState(orderInfo.user_id.toString,"1" ))
      //import org.apache.phoenix.spark._ 该方法增加了rdd的saveToPhoenix方法
      newConsumedUserRDD.saveToPhoenix("USER_STATE2020",Seq("USER_ID","IF_CONSUMED"),
        new Configuration,Some("hdp4.buptnsrc.com:2181"))


      OffsetManger.setOffset(topic,groupid,offsetRanges)
    }
    //8.2 保存到es中
    orderInfoWithUserDstream.cache()
    orderInfoWithUserDstream.print(100)
    orderInfoWithUserDstream.foreachRDD{rdd=>
      rdd.foreachPartition{ orderInfoItr=>
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        val orderInfoWithIdList: List[(String, OrderInfo)] = orderInfoList.map(orderInfo=>(orderInfo.id.toString,orderInfo))
        val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        //发送到es
        MyEsUtil.bulkDoc(orderInfoWithIdList,"gmall2020_order_info_"+dateString)
        //发送到kafka中
        for (orderInfo <- orderInfoList ) {
          val orderInfoJsonString: String  = JSON.toJSONString(orderInfo,new SerializeConfig(true))
          MyKafkaSink.send("DWD_ORDER_INFO", orderInfoJsonString)
        }

      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
