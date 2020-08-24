package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    Double getTotalAmount(String date);

    List<Map<String, Object>> getHourAmount(String date);
}
