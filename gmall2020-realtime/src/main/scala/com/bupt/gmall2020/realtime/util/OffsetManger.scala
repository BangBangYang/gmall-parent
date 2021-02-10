package com.bupt.gmall2020.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis



/**
 * @author yangkun
 * @date 2021/2/2 11:17
 * @version 1.0
 */
object OffsetManger {
  //取offset
  // Redis 中偏移量的保存格式   type?  hash   key ?  "offset:[topic]:[groupid]"    field ?  partition_id  value ?  offset     expire
  def getOffset(topicName:String,groupId:String): Map[TopicPartition,Long] ={
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey="offset:"+topicName+":"+groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import scala.collection.JavaConversions._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.map { case (partionId, offset) =>
      println("加载分区偏移量："+partionId +":"+offset  )
      (new TopicPartition(topicName, partionId.toInt), offset.toLong)
    }.toMap
    kafkaOffsetMap
  }
  //存offset
  // Redis 中偏移量的保存格式   type?  hash   key ?  "offset:[topic]:[groupid]"    field ?  partition_id  value ?  offset     expire
  def setOffset(topicName:String,groupId:String,offsetRanges:Array[OffsetRange]): Unit ={
    val offsetKey="offset:"+topicName+":"+groupId
    val offsetMap:util.Map[String,String] = new util.HashMap[String,String]()
    for( offset <- offsetRanges){
      offsetMap.put(offset.partition+"",offset.untilOffset+"")
//      println("写入分区："+offset.partition +":"+offset.fromOffset+"-->"+offset.untilOffset)
    }
    if(offsetMap!=null && offsetMap.size() > 0) {
      val jedis: Jedis = RedisUtil.getJedisClient
      jedis.hmset(offsetKey,offsetMap)
     jedis.close()
   }


  }

}
