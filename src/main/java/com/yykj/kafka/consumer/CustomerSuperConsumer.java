package com.yykj.kafka.consumer;

import java.util.Arrays;
import java.util.Collections;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import kafka.javaapi.consumer.SimpleConsumer;

public class CustomerSuperConsumer {

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		//获取配置信息
		Properties prop = GetProperties();
		
		//生成Consumer对象
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
		
		//指定消费Topic集合
		//consumer.subscribe(Arrays.asList("first","second","thrid"));
		consumer.subscribe(Collections.singletonList("first"));
		
		//新版本指定offset读取消息 *注释consumer.subscribe行，不可重复指定topic;
		//consumer.assign(Collections.singletonList(new TopicPartition("first", 0)));
		//consumer.seek(new TopicPartition("first", 0), 1);
		
		//输出
		while(true){
			
			//拉取数据
			ConsumerRecords<String, String> records = consumer.poll(100);
		
			for (ConsumerRecord<String, String> info : records) {
				System.out.println("topic:" + info.topic() + " partition:" + info.partition() + " value:" + info.value());
			}
		}
	}

	/**
	 * 
	 * @return
	 */
	public static Properties GetProperties() {
		//配置信息
		Properties prop = new Properties();
		//集群
		prop.put("bootstrap.servers", "node21:9092");
		//消费者组
		prop.put("group.id", "0");
		//重复消费可以将消费者更换组，此配置将重置后从开始读取消息
		//prop.put("auto.offset.reset", "earliest");
		//自动提交offset（消费到什么位置）
		prop.put("enable.auto.commit", "true");
		//提交延时 小心重复消费
		prop.put("auto.commit.interval.ms", "1000");
		//序列化 反序列化
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		
//		prop.put("auto.commit.interval.ms", "1000");
//		prop.put("zookeeper.session.timeout.ms", "4000");
//		prop.put("zookeeper.sync.time.ms", "200");
//		prop.put("zookeeper.connect", "192.168.61.151:2181,192.168.61.152:2181,192.168.61.153:2181");
		
		return prop;
	}

}
