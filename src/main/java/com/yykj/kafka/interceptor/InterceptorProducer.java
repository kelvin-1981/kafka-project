package com.yykj.kafka.interceptor;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class InterceptorProducer {

	public static void main(String[] args) {

		// 创建生产者配置
		Properties props = getProperties();
		
		//配置拦截器
		ArrayList<String> inter_list = new ArrayList<String>();
		inter_list.add("com.yykj.kafka.interceptor.TimeInterceptor");
		inter_list.add("com.yykj.kafka.interceptor.CountInterceptor");
		
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, inter_list);

		// 创建生产者对象
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		for(int i = 0; i < 10; i ++){
			
			producer.send(new ProducerRecord<String, String>("first", String.valueOf(i)), new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null){
						System.out.println("partition:" + metadata.partition() + " offset:" + metadata.offset());
					}
					else{
						System.out.println("Fail");
					}
				}
			});
		}
		
		producer.close();
	}

	/**
	 * 生产者配置对象
	 * 
	 * @return
	 */
	public static Properties getProperties() {
		// 构造一个java.util.Properties对象
		Properties props = new Properties();

		// 指定bootstrap.servers属性。必填，无默认值。用于创建向kafka broker服务器的连接。
		props.put("bootstrap.servers", "node21:9092");
		// props.put("bootstrap.servers",
		// "node21:9092,node22:9092,node23:9092");

		// acks参数用于控制producer生产消息的持久性（durability）。参数可选值，0、1、-1（all）。
		props.put("acks", "all");

		// props.put(ProducerConfig.ACKS_CONFIG, "1");
		// 在producer内部自动实现了消息重新发送。默认值0代表不进行重试。
		props.put("retries", 3);

		// props.put(ProducerConfig.RETRIES_CONFIG, 3);
		// 调优producer吞吐量和延时性能指标都有非常重要作用。默认值16384即16KB。
		props.put("batch.size", 16384);

		// props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		// 控制消息发送延时行为的，该参数默认值是0。表示消息需要被立即发送，无须关系batch是否被填满。
		props.put("linger.ms", 1);

		// props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		// 指定了producer端用于缓存消息的缓冲区的大小，单位是字节，默认值是33554432即32M。
		props.put("buffer.memory", 33554432);

		// 指定key.serializer属性。必填，无默认值。被发送到broker端的任何消息的格式都必须是字节数组。
		// 因此消息的各个组件都必须首先做序列化，然后才能发送到broker。该参数就是为消息的key做序列化只用的。
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 指定value.serializer属性。必填，无默认值。和key.serializer类似。此被用来对消息体即消息value部分做序列化。
		// 将消息value部分转换成字节数组。
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		// props.put("max.block.ms", 3000);
		// props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		// 设置producer段是否压缩消息，默认值是none。即不压缩消息。GZIP、Snappy、LZ4
		// props.put("compression.type", "none");
		// props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
		// 该参数用于控制producer发送请求的大小。producer端能够发送的最大消息大小。
		// props.put("max.request.size", 10485760);
		// props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);
		// producer发送请求给broker后，broker需要在规定时间范围内将处理结果返还给producer。默认30s
		// props.put("request.timeout.ms", 60000);
		// props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);

		return props;
	}

}
