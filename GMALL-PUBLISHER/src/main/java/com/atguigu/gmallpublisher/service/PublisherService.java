package com.atguigu.gmallpublisher.service;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public interface PublisherService {
    long getDau(String date);
    HashMap<String, Long> getHourDau(String date);

    Double getTotalAmount(String date);
    HashMap<String, Double> getHourAmount(String date);

    /**
     * 总数，
     * 聚合结果 : 年龄、性别
     * 详情 :
     *
     * Map(String -> Object)
     * @param date
     * @param keyword
     * @param startpage
     * @param size
     */
    Map<String, Object> getSaleDetailAndAgg(String date,
                                            String keyword,
                                            int startpage,
                                            int size
                             ) throws IOException;
}
