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

## scala 读取配置文件的开发
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
