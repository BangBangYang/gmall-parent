package com.bupt.gmall2020.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.bupt.gmall2020.realtime.bean.OrderDetail
import com.bupt.gmall2020.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManger, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yangkun
 * @date 2021/2/13 13:17
 * @version 1.0
 */
object OrderDetailApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("order_detail_app").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    //加载流数据
    val topic:String = "ODS_ORDER_DETAIL"
    val groupId:String = "order_detail_group"
    //获取redis中的offset
    val offsetMap: Map[TopicPartition, Long] = OffsetManger.getOffset(topic,groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size > 0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //得到kafka中的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    recordInputStream.transform{rdd=>
       offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val orderDetailDstream: DStream[OrderDetail] = recordInputStream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }
    // 合并维表数据
    // 品牌 分类 spu  作业
    //  orderDetailDstream.
    /////////////// 合并 商品信息////////////////////

    val orderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDstream.mapPartitions { orderDetailItr =>
      val orderDetailList: List[OrderDetail] = orderDetailItr.toList
      if(orderDetailList.size>0) {
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        val sql = "select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from gmall2020_sku_info  where id in ('" + skuIdList.mkString("','") + "')"
        val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val skuJsonObjMap: Map[Long, JSONObject] = skuJsonObjList.map(skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)).toMap
        for (orderDetail <- orderDetailList) {
          val skuJsonObj: JSONObject = skuJsonObjMap.getOrElse(orderDetail.sku_id, null)
          orderDetail.spu_id = skuJsonObj.getLong("SPU_ID")
          orderDetail.spu_name = skuJsonObj.getString("SPU_NAME")
          orderDetail.tm_id = skuJsonObj.getLong("TM_ID")
          orderDetail.tm_name = skuJsonObj.getString("TM_NAME")
          orderDetail.category3_id = skuJsonObj.getLong("CATEGORY3_ID")
          orderDetail.category3_name = skuJsonObj.getString("CATEGORY3_NAME")
        }
      }
      orderDetailList.toIterator
    }


    orderDetailWithSkuDstream.cache()
    orderDetailWithSkuDstream.print(100)
    orderDetailWithSkuDstream.foreachRDD{rdd=>
      rdd.foreach{ orderDetail=>
        val orderDetailJsonString: String  = JSON.toJSONString(orderDetail,new SerializeConfig(true))
        MyKafkaSink.send("DWD_ORDER_DETAIL",orderDetailJsonString)
      }

      OffsetManger.setOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
