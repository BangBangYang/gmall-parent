package com.bupt.gmall2020.realtime.dws

import java.lang

import com.alibaba.fastjson.JSON
import com.bupt.gmall2020.realtime.bean.{OrderDetail, OrderInfo}
import com.bupt.gmall2020.realtime.util.{MyKafkaUtil, OffsetManger, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2021/2/13 13:18
 * @version 1.0
 */
object OrderDetailWideApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("order_wide_app").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    val topicOrderInfo = "DWD_ORDER_INFO"
    val topicOrderDetail = "DWD_ORDER_DETAIL"
    val groupIdOrderInfo = "dws_order_info_group"
    val groupIdOrderDetail = "dws_order_detail_group"

    // 订单主表
    val orderInfokafkaOffsetMap: Map[TopicPartition, Long] = OffsetManger.getOffset(topicOrderInfo, groupIdOrderInfo)
    var orderInfoRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfokafkaOffsetMap != null && orderInfokafkaOffsetMap.size > 0) {
      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, orderInfokafkaOffsetMap, groupIdOrderInfo)
    } else {
      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupIdOrderInfo)
    }

    // 订单明细
    val orderDetailkafkaOffsetMap: Map[TopicPartition, Long] = OffsetManger.getOffset(topicOrderDetail, groupIdOrderDetail)
    var orderDetailRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailkafkaOffsetMap != null && orderDetailkafkaOffsetMap.size > 0) {
      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, orderDetailkafkaOffsetMap, groupIdOrderDetail)
    } else {
      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupIdOrderDetail)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputStream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputStream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    ////////////////////////
    ///// 1.结构调整  //////
    ///////////////////////
    //1.
    val orderDetailStream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }

    val orderInfoStream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val orderIno: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderIno
    }

//    orderInfoStream.print(1000)
//    orderDetailStream.print(1000)
    //2 转化为
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailStream.map(orderDetail=>(orderDetail.order_id,orderDetail))
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoStream.map(orderInfo=>(orderInfo.id,orderInfo))
    //如果单纯join无法保证应对配对的主表和从表数据都在一个批次中，join有可能丢失数据
//    val test: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)
//    test.print(100)


    //3 开窗 解决数据主表和从表数据不在一个批次中，但是会造成数据重复问题，滑动窗口会重复覆盖数据(比如10s窗口，滑动步长是5s，每份数据会重复两份)
    val orderInfoWithKeyWindowStream: DStream[(Long, OrderInfo)] = orderInfoWithKeyDstream.window(Seconds(10),Seconds(5))
    val orderDetailWithKeyWindowStream: DStream[(Long, OrderDetail)] = orderDetailWithKeyDstream.window(Seconds(10),Seconds(5))
//    //4 开窗join
    val  orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowStream.join(orderDetailWithKeyWindowStream)
    //解决开窗join数据重复问题
    val  orderJoinedNewDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "order_join_keys"
      val orderJoinedNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]()
      for ((orderId, (orderInfo, orderDetail)) <- orderJoinedTupleItr) {
        //Redis  type? set  key order_join_keys   value    orderDetail.id
        val ifNew: lang.Long = jedis.sadd(key, orderDetail.id.toString)

        if (ifNew == 1L) {
          orderJoinedNewList.append((orderId, (orderInfo, orderDetail)))
        }
      }
      jedis.close()
      orderJoinedNewList.toIterator

    }
     orderJoinedNewDstream.print(1000) //去重数据

//    orderJoinedDstream.print(100) //未去重数据
    ssc.start()
    ssc.awaitTermination()
  }

}
