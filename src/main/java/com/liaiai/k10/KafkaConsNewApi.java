package com.liaiai.k10;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by lilaizhen on 16/9/6.
 */
public class KafkaConsNewApi {
    public static void consumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "123.57.84.60:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");  //自动commit
        props.put("auto.commit.interval.ms", "1000"); //定时commit的周期
        props.put("session.timeout.ms", "30000"); //consumer活性超时时间
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-01")); //subscribe，foo，bar，两个topic
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);  //poll 100 条 records
            for (ConsumerRecord<String, String> record : records){
                System.out.println(record.topic()+"#"+ record.offset()+"#"+record.key()+"#"+record.value());
            }
        }
    }

    public static void main(String[] args) {
        consumer();
    }
}
