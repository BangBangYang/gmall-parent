###1、searchResult.getTotal()查询es7中 报java.lang.UnsupportedOperationException: JsonObject
利用jestclient查询es7中数据改方法会出现bug

因为es7返回json串的格式出现了一些改变
```$xslt
ES V7 以前格式
hits: {
total: 16561096
}

ES7 现在返回格式
hits: {
total: {
value: 49
}
}
```

jestClient的SearchResult的getTotal()方法还在使用
public static final String[] PATH_TO_TOTAL = "hits/total".split("/");
解析,因此在getTotal()不会解析成一个数值
其实应该变成
public static final String[] PATH_TO_TOTAL = "hits/total/value".split("/");
解决方法
```$xslt
SearchResult searchResult = jestClient.execute(search);
JsonObject jsonObject = searchResult.getJsonObject();
JsonElement jsonElement = jsonObject.get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value");
return  jsonElement.getAsLong();
```

## 2、scala 读取配置文件的开发
```$xslt
package com.bupt.gmall2020.realtime.util

import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @author yangkun
 * @date 2021/2/1 14:24
 * @version 1.0
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {

   val prop =  load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
    println(classOf[StringDeserializer])
  }
  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}



```

## 3、phoenix
### 3.1 spark 写入phoenix
先通过sql语句创建表
```$xslt
create table gmall2020_province_info  ( id varchar primary key , info.name  varchar , info.area_code varchar , info.iso_code varchar,info.iso_3166_2 varchar )SALT_BUCKETS = 3;
```
1、需要引入 import org.apache.phoenix.spark._ 这个隐式转化
2、maven包
```$xslt
<dependency>
            <groupId>org.apache.phoenix</groupId>
            <artifactId>phoenix-spark</artifactId>
            <version>5.0.0-HBase-2.0</version>
        </dependency>
```
3、示例代码
```$xslt
     provinceRDD.saveToPhoenix("gmall2020_province_info",Seq("ID" , "NAME" , "AREA_CODE","ISO_CODE","ISO_3166_2")
        ,new Configuration
        ,Some("hdp4.buptnsrc.com:2181"))
```
5、利用maxwell-bootstrap 初始化数据
将mysql原本存在的数据全部送到kafka的gmall_DB_M中
```$xslt
bin/maxwell-bootstrap --user maxwell  --password maxwell --host localhost  --database gmall --table base_province --client_id maxwell_1
```
 1)BaseDSMaxwell 将数据从gmall_DB_M打到ODS_BASE_PROVINCE中
 2）ProvinceApp, 将数据从ODS_BASE_PROVINCE 写入到phoenix中
 
 6、spark数据写入es中
 6.1、利用kibana建立索引
 ```$xslt
PUT   _template/gmall2020_order_info_template
{
  "index_patterns": ["gmall2020_order_info*"],                  
  "settings": {                                   
    "number_of_shards": 3
  },
  "aliases" : { 
    "{index}-query": {},
    "gmall2020_order_info-query":{}
  },
   "mappings": {
     
       "properties":{
         "id":{
           "type":"long"
         },
         "province_id":{
           "type":"long"
         },
         "order_status":{
           "type":"keyword"
         },
         "user_id":{
           "type":"long"
         },
         "final_total_amount":{
           "type":"double"
         },
          "benefit_reduce_amount":{
           "type":"double"
         },
          "original_total_amount":{
           "type":"double"
         },
          "feight_fee":{
           "type":"double"
         },
          "expire_time":{
           "type":"keyword"
         },
          "create_time":{
           "type":"keyword"
         },
         "create_date":{
           "type":"date"
         },
         "create_hour":{
           "type":"keyword"
         },
         "if_first_order":{
           "type":"keyword"
         },
         "province_name":{
           "type":"keyword"
         },
          "province_area_code":{
           "type":"keyword"
         },
         "province_iso_code":{
                   "type":"keyword"
         },
         "province_iso_3166_2":{
                   "type":"keyword"
         },
         "user_age_group":{
           "type":"keyword"
         },
         "user_gender":{
           "type":"keyword"
         }  
       }
     }
   
}

```
6.2 EsUtils

```$xslt
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
``` 
6.3 maven 依赖
```$xslt
<dependency>
            <groupId>io.searchbox</groupId>
            <artifactId>jest</artifactId>
            <version>5.3.3</version>
            <exclusions>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-api</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <dependency>
            <groupId>net.java.dev.jna</groupId>
            <artifactId>jna</artifactId>
            <version>4.5.2</version>
        </dependency>

        <!--        <dependency>-->
        <!--            <groupId>org.codehaus.janino</groupId>-->
        <!--            <artifactId>commons-compiler</artifactId>-->
        <!--            <version>2.7.8</version>-->
        <!--        </dependency>-->

        <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch</artifactId>
            <version>2.4.6</version>
        </dependency>
```
