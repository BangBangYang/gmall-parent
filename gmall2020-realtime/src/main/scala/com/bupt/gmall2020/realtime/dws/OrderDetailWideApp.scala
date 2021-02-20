package com.bupt.gmall2020.realtime.dws

import java.util.Properties
import java.{lang, util}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.bupt.gmall2020.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.bupt.gmall2020.realtime.util.{MyKafkaUtil, OffsetManger, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
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
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
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
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailStream.map(orderDetail => (orderDetail.order_id, orderDetail))
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoStream.map(orderInfo => (orderInfo.id, orderInfo))
    //如果单纯join无法保证应对配对的主表和从表数据都在一个批次中，join有可能丢失数据
    //    val test: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)
    //    test.print(100)


    //3 开窗 解决数据主表和从表数据不在一个批次中，但是会造成数据重复问题，滑动窗口会重复覆盖数据(比如10s窗口，滑动步长是5s，每份数据会重复两份)
    val orderInfoWithKeyWindowStream: DStream[(Long, OrderInfo)] = orderInfoWithKeyDstream.window(Seconds(10), Seconds(5))
    val orderDetailWithKeyWindowStream: DStream[(Long, OrderDetail)] = orderDetailWithKeyDstream.window(Seconds(10), Seconds(5))
    //    //4 开窗join
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowStream.join(orderDetailWithKeyWindowStream)
    //解决开窗join数据重复问题
    val orderJoinedNewDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>
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

    //    orderJoinedDstream.print(100) //未去重数据
    //    orderJoinedNewDstream.print(1000) //去重数后的数据
    val orderDetailWideDStream: DStream[OrderDetailWide] = orderJoinedNewDstream.map { case (orderId, (orderInfo, orderDetail)) => new OrderDetailWide(orderInfo, orderDetail) }
//    orderDetailWideDStream.print(1000)
    /////////////////////////////////////////////////////////
    // 计算实付分摊需求
    ////////////////////////////////////////
    // 思路 ：：
    //    每条明细已有        1  原始总金额（original_total_amount） （明细单价和各个数的汇总值）
    //    2  实付总金额 (final_total_amount)  原始金额-优惠金额+运费
    //    3  购买数量 （sku_num)
    //    4  单价      ( order_price)
    //
    //    求 每条明细的实付分摊金额（按明细消费金额比例拆分）
    //
    //    1  33.33   40    120
    //    2  33.33   40    120
    //    3   ？     40    120
    //
    //    如果 计算是该明细不是最后一笔
    //      使用乘除法      实付分摊金额/实付总金额= （数量*单价）/原始总金额
    //      调整移项可得  实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
    //
    //    如果  计算时该明细是最后一笔
    //      使用减法          实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //    1 减法公式
    //      2 如何判断是最后一笔
    //      如果 该条明细 （数量*单价）== 原始总金额 -（其他明细【数量*单价】的合计）
    //
    //
    //    两个合计值 如何处理
    //      在依次计算的过程中把  订单的已经计算完的明细的【实付分摊金额】的合计
    //    订单的已经计算完的明细的【数量*单价】的合计
    //    保存在redis中 key设计
    //    type ?   hash      key? order_split_amount:[order_id]  field split_amount_sum ,origin_amount_sum    value  ?  累积金额

    //  伪代码
    //    1  先从redis取 两个合计    【实付分摊金额】的合计，【数量*单价】的合计
    //    //    2 先判断是否是最后一笔  ： （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
    //    //    3.1  如果不是最后一笔：
    //                      // 用乘除计算 ： 实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
    //
    //    //    3.2 如果是最后一笔
    //                     // 使用减法 ：   实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //    //    4  进行合计保存
    //              //  hincr
    ////              【实付分摊金额】的合计，【数量*单价】的合计

    val orderWideWithSplitDstream = orderDetailWideDStream.mapPartitions{orderWideItr=>
      val jedis: Jedis = RedisUtil.getJedisClient

      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      for(orderWide <- orderWideList){

          val key: String = "order_split_amount:"+orderWide.order_id
          val orderSumMap: util.Map[String, String] = jedis.hgetAll(key)
          var splitAmountSum = 0D
          var originAmountSum = 0D
          //从redis 得到该订单的已经分摊合计金额和原始的合计金额
          if(orderSumMap != null && orderSumMap.size() > 0){
            val splitAmountStr: String = orderSumMap.get("split_amount_sum")
            splitAmountSum = splitAmountStr.toDouble
            val originMountStr:String = orderSumMap.get("origin_amount_sum")
            originAmountSum = originMountStr.toDouble
          }
        //判断是否为最后一个订单
        //    2 先判断是否是最后一笔  ： （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
        val detailOrginAmount: Double = orderWide.sku_num * orderWide.sku_price //单条明细的原始金额  数量*单价
        val restOriginAmount: Double = orderWide.final_total_amount - originAmountSum
        if (detailOrginAmount == restOriginAmount) {
          //3.1  最后一笔 用减法 ：实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
          orderWide.final_detail_amount = orderWide.final_total_amount - splitAmountSum
        } else {
          //3.2  不是最后一笔 用乘除  实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
          orderWide.final_detail_amount = detailOrginAmount * orderWide.final_total_amount / orderWide.original_total_amount
          orderWide.final_detail_amount= Math.round(orderWide.final_detail_amount*100D)/100D //保留两位小数
        }

        //    4  进行合计保存
        splitAmountSum += orderWide.final_detail_amount
        originAmountSum += detailOrginAmount
        orderSumMap.put("split_amount_sum", splitAmountSum.toString)
        orderSumMap.put("origin_amount_sum", originAmountSum.toString)
        jedis.hmset(key, orderSumMap)

      }
      jedis.close()
      //4  进行合计保存
      orderWideList.toIterator
    }
//    orderWideWithSplitDstream.print(1000)
    orderWideWithSplitDstream.cache()
    orderWideWithSplitDstream.map(orderwide=>JSON.toJSONString(orderwide,new SerializeConfig(true))).print(1000)
    val sparkSessoin= SparkSession.builder()
                                  .appName("order_detail_wide_session")
                                  .getOrCreate()

    //写入clickhouse
    import sparkSessoin.implicits._
    orderWideWithSplitDstream.foreachRDD{rdd=>
      val df = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "1") // 设置并发
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://10.108.113.211:8123/test","order_wide",new Properties())

      OffsetManger.setOffset(topicOrderInfo,groupIdOrderInfo,orderInfoOffsetRanges)
      OffsetManger.setOffset(topicOrderDetail,groupIdOrderDetail,orderDetailOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
