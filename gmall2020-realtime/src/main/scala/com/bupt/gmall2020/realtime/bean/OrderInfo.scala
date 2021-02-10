package com.bupt.gmall2020.realtime.bean

/**
 * @author yangkun
 * @date 2021/2/8 22:50
 * @version 1.0
 */
case class OrderInfo(
                      id: Long,
                      province_id: Long,
                      order_status: String,
                      user_id: Long,
                      final_total_amount: Double,
                      benefit_reduce_amount: Double,
                      original_total_amount: Double,
                      feight_fee: Double,
                      expire_time: String,
                      create_time: String,
                      operate_time: String,
                      var create_date: String,
                      var create_hour: String,
                      var if_first_order:String,

                      var province_name:String,
                      var province_area_code:String,
                      var province_iso_code:String,
                      var province_iso_3166_2:String,

                      var user_age_group:String,
                      var user_gender:String

                    )


