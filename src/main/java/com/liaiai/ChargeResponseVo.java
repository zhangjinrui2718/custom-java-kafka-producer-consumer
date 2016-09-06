package com.liaiai;

import lombok.*;

import java.math.BigDecimal;

/**
 * @author linjunbo
 * @version V1.0
 * @Title: ChargeResponseVo.java
 * @Package com.wentianxia.module.vo
 * @Description: 计费响应VO
 * @date 2016年8月31日 下午12:01:40
 */
@lombok.Data
@ToString
public class ChargeResponseVo {
    private String appId;
    private String distribution;
    private BigDecimal amount;
    private Long orderId;
    private String settingId;
    private String payerId;
    private String payerName;
    private String payHeadImg;
    private String liveUserId;
}
