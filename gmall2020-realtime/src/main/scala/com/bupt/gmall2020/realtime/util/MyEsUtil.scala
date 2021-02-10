package com.bupt.gmall2020.realtime.util
import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

/**
 * @author yangkun
 * @date 2021/1/29 17:33
 * @version 1.0
 */
object MyEsUtil {
  var factory:JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://10.108.113.211:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())

  }
  //在处理完成对redis自动提交offset后，es利用id 进行幂等处理（因为假如sparkStreaming在处理过程中崩溃了，在启动可能会存在重复数据问题）
  def bulkDoc(sourceList:List[(String,Any)],indexName:String): Unit ={
    if(sourceList.size ==0 || sourceList == null)
      return
    val jest: JestClient = getClient
    val bulkBuilder: Bulk.Builder = new Bulk.Builder
    for((id,source) <- sourceList){
        val index: Index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
      bulkBuilder.addAction(index)
    }
    val bulk: Bulk = bulkBuilder.build()
    val result: BulkResult = jest.execute(bulk)
    val items: util.List[BulkResult#BulkResultItem] = result.getItems
    println("保存到ES:"+items.size()+"条数")
    jest.close()

  }
 def addDoc(): Unit ={
   val jest: JestClient = getClient
    val index: Index = new Index.Builder(Movie2020("1001","天下","杨坤")).index("movie_test_20210121").`type`("_doc").id("1").build()
   val message: String = jest.execute(index).getErrorMessage
   if(message != null){
     println(message)
   }
  jest.close()

 }
  // 把结构封装的Map 必须使用java 的   ，不能使用scala
  def queryDoc(): Unit ={
    val jest: JestClient = getClient
//    val query="{\n  \"query\": {\n    \"bool\": {\n      \"must\": [\n        { \"match\": {\n          \"name\": \"operation\"\n        }}\n      ],\n      \"filter\": {\n            \"term\": {\n           \"actorList.name.keyword\": \"zhang han yu\"\n         }\n      }\n    }\n  },\n  \"from\": 0\n  , \"size\": 20\n  ,\"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ]\n \n}"
val query =
  """
    |{
    |
    |  "query":{
    |      "bool":{
    |          "must":[
    |            {"match":{"name":"red"}  }
    |            ,
    |            {"match":{"id":1}}
    |          ]
    |          ,
    |          "filter":{
    |              "term":{
    |                  "actorList.name.keyword":"zhang han yu"
    |            }
    |          }
    |
    |      }
    |  }
    |}
    |""".stripMargin
    val searchSourceBuilder = new SearchSourceBuilder()

    val boolQueryBuilder = new BoolQueryBuilder
    boolQueryBuilder.must(new MatchQueryBuilder("name","red"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","zhang han yu"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0).size(20)
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)

    val query2: String = searchSourceBuilder.toString

//    println(query2)
    val search= new Search.Builder(query2).addIndex("movie_index").addType("movie").build()
    val result: SearchResult = jest.execute(search)
//    println("****************"+result.getTotal)
    val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits( classOf[util.Map[String,Any]])
    import scala.collection.JavaConversions._
    for (hit <- hits ) {

      println(hit.source.mkString(","))
    }

    jest.close()
  }
  def main(args: Array[String]): Unit = {
//    addDoc()
   queryDoc()
  }

}
case class Movie2020(id:String,movie_name:String,name:String)
