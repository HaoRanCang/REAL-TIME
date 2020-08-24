package com.atguigu.gmallpublisher.service;


import java.util.HashMap;


public interface PublisherService {
    long getDau(String date);
    HashMap<String, Long> getHourDau(String date);

    Double getTotalAmount(String date);
    HashMap<String, Double> getHourAmount(String date);


}
