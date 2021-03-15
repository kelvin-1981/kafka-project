package com.yykj.kafka.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CountInterceptor  implements ProducerInterceptor<String,String>{

	private int successCount = 0;
	
	private int failCount = 0;
	
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		// TODO Auto-generated method stub
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		if(exception == null){
			successCount += 1;
		}
		else{
			failCount += 1;
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		System.out.println("success:" + successCount);
		
		System.out.println("fail:" + failCount);
	}

}
