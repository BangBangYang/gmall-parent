package com.bupt.gmall2020.realtime.bean.dim

/**
 * @author yangkun
 * @date 2021/2/10 19:35
 * @version 1.0
 */
case class ProvinceInfo(id:String,
                        name:String,
                        area_code:String,
                        iso_code:String,
                        iso_3166_2:String
                       ) {
  override def toString: String = "id: "+id+" name: "+name+" iso_code: "+iso_code+" iso_3166: "+iso_3166_2

}
