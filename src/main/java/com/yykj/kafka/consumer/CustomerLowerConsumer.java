package com.yykj.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;


/**
 * 
 * @author YYKJ
 *
 */
@SuppressWarnings("deprecation")
public class CustomerLowerConsumer {

	// broker list
    private static final String BROKER_LIST = "node21";
    //端口号
    private static final int PORT = 9092;
    // 连接超时时间：1min
    private static final int TIME_OUT = 60 * 1000;
    // 读取消息缓存区大小：1M
    private static final int BUFFER_SIZE = 1024 * 1024;
    // 每次获取消息的条数
    private static final int FETCH_SIZE = 10000000;
    // 发生错误时重试的次数
    private static final int RETRIES_TIME = 3;
    // 允许发生错误的最大次数
    private static final int MAX_ERROR_NUM = 3;
    
	/**
	 * 使用低级API 指定Topic Partition Offset 获取数据
	 * @param args
	 */
	public static void main(String[] args) {
		
		String topic = "first";
		
		int partition = 0;
		
		long offset = 5;
		
		//1.寻找指定Topic下Partition的Leader
		BrokerEndPoint leader = findLeader(Arrays.asList(BROKER_LIST.split(",")), PORT, topic, partition);
		if(leader == null){
			System.out.println("Fail : findLeader()");
		}
		
		
		//获取数据
		getOffsetData(leader.host(), topic, partition, offset);
	}
	
	/**
	 * 获取Partition Leader
	 * @return
	 */
	@SuppressWarnings({ "deprecation" })
	private static BrokerEndPoint findLeader(List<String> brokers, int port, String topic,int partition) {
		
		for (String host : brokers) {
			
			//获取broker下的消费者对象
			SimpleConsumer consumer = new SimpleConsumer(host, PORT, TIME_OUT, BUFFER_SIZE, "getLeader");
			
			//创建元数据请求对象
			TopicMetadataRequest request = new kafka.javaapi.TopicMetadataRequest(Collections.singletonList(topic));
			
			//发送元数据请求
			TopicMetadataResponse metaResponse = consumer.send(request);
			
			//获取元数据信息
			List<TopicMetadata> topicsMetadata = metaResponse.topicsMetadata();
			for (TopicMetadata topicMeta : topicsMetadata) {
				
				//寻找对应Topic
				if(!topic.equals(topicMeta.topic())){
					continue;
				}

				//获取分区元数据信息
				List<PartitionMetadata> partitionsMetadata = topicMeta.partitionsMetadata();
				for (PartitionMetadata partMeta : partitionsMetadata) {
					//寻找对应Partition
					if(partMeta.partitionId() != partition){
						continue;
					}
					//获取该分区的leader
					return partMeta.leader();
				}
			}
		}
		return null;
	}

	//获取数据
	@SuppressWarnings("deprecation")
	private static void getOffsetData(String host,String topic,int partition,long offset) {
		
		SimpleConsumer consumer = new SimpleConsumer(host, PORT, TIME_OUT, BUFFER_SIZE, "getData");
		
		FetchRequest request = new FetchRequestBuilder().addFetch(topic, partition, offset, FETCH_SIZE).build();
		
		FetchResponse fetch = consumer.fetch(request);
		
		ByteBufferMessageSet messageSet = fetch.messageSet(topic, partition);
		
		for (MessageAndOffset messageAndOffset : messageSet) {
			
//			if(messageAndOffset.offset() != offset){
//				continue;
//			}
			
			long offset2 = messageAndOffset.offset();
			
			ByteBuffer payload = messageAndOffset.message().payload();
			
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			
			System.out.println("offset:" + offset2 + " value:" + new String(bytes));
		}
	}
}
