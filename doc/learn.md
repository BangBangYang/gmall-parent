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