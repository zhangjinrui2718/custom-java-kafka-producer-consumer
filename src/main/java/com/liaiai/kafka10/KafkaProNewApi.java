package com.liaiai.kafka10;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 这个是0.10版本客户端的生产者代码，提供了回调的send方法 直接连接kafka
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
        String data="{taskId: 164,tokenArray: [" +
                "\"308187f0904411e6830419bbff96e759\",\"dsarewfafas\",\"3erdasd\"]" +
                "}";
        ProducerRecord<String,String> record = new ProducerRecord<>("#","1", data);
            producer.send(record,
                    (metadata, e) -> {
                        if(e != null){
                            e.printStackTrace();
                        }
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                    });
            TimeUnit.SECONDS.sleep(1);
//            producer.send(new ProducerRecord<>("bar", Integer.toString(i), Integer.toString(i)));
        producer.close();

    }

    public static void main(String[] args) throws InterruptedException {
        send();
    }
}
