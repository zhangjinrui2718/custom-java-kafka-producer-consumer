package com.liaiai.k10;

import com.alibaba.fastjson.JSON;
import com.liaiai.ChargeResponseVo;
import org.apache.kafka.clients.producer.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by lilaizhen on 16/9/6.
 */
public class KafkaProNewApi {
    public static void send() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "#");
        props.put("acks", "all"); //ack方式，all，会等所有的commit最慢的方式
        props.put("retries", 0); //失败是否重试，设置会有可能产生重复数据
        props.put("batch.size", 16384); //对于每个partition的batch buffer大小
        props.put("linger.ms", 1);  //等多久，如果buffer没满，比如设为1，即消息发送会多1ms的延迟，如果buffer没满
        props.put("buffer.memory", 33554432); //整个producer可以用于buffer的内存大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 100; i++){
            ChargeResponseVo chargeResponseVo=new ChargeResponseVo();
            chargeResponseVo.setPayerId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
            chargeResponseVo.setAmount(new BigDecimal("12.1"));
            chargeResponseVo.setOrderId(100000462l);
            chargeResponseVo.setLiveUserId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
            chargeResponseVo.setPayHeadImg("http://test.fada.cim/qdja/skaldksaldk.jpg");
            chargeResponseVo.setSettingId("dda");
            chargeResponseVo.setDistribution("asda");
            chargeResponseVo.setAppId("wtx");

            ChargeResponseVo chargeResponseVo2=new ChargeResponseVo();
            chargeResponseVo2.setPayerId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
            chargeResponseVo2.setAmount(new BigDecimal("12.1"));
            chargeResponseVo2.setOrderId(100000462l);
            chargeResponseVo2.setLiveUserId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
            chargeResponseVo2.setPayHeadImg("http://test.fada.cim/qdja/skaldksaldk.jpg");
            chargeResponseVo2.setSettingId("dda");
            chargeResponseVo2.setDistribution("asda");
            chargeResponseVo2.setAppId("wtx");

            List<ChargeResponseVo> chargeResponseVos=new ArrayList<>();
            chargeResponseVos.add(chargeResponseVo);
            chargeResponseVos.add(chargeResponseVo2);
            ProducerRecord<String,String> record = new ProducerRecord<>("test-01","1", JSON.toJSONString(chargeResponseVos));
            producer.send(record,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null){
                                e.printStackTrace();
                            }
                            System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        }
                    });
            TimeUnit.SECONDS.sleep(1);
//            producer.send(new ProducerRecord<>("bar", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();

    }

    public static void main(String[] args) throws InterruptedException {
        send();
    }
}
