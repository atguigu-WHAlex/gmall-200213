package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaSender {

    //声明生产者
    private static KafkaProducer<String, String> producer;

    //创建生产者
    private static KafkaProducer<String, String> createProducer() {

        //1.创建配置信息
        Properties properties = new Properties();

        //2.添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        //3.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //4.返回生产者
        return producer;
    }


    //将数据发送至Kafka
    public static void send(String topic, String msg) {

        //判断生产者是否为空,如果为空,则创建
        if (producer == null) {
            producer = createProducer();
        }

        //发送数据
        producer.send(new ProducerRecord<>(topic, msg));
    }

}
