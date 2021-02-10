package com.bupt.gmall2020.publisher.service;

import java.util.Map;

/**
 * @author yangkun
 * @version 1.0
 * @date 2021/2/5 12:32
 */
public interface EsService {

    public Long getDauTotal(String date);

    public Map getDauHour(String date);
}
