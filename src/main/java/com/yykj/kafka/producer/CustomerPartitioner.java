package com.yykj.kafka.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;


/**
 * 自定义分区
 * @author YYKJ
 *
 */
public class CustomerPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int partition(String arg0, Object arg1, byte[] arg2, Object arg3, byte[] arg4, Cluster arg5) {
		// TODO Auto-generated method stub
		return 0;
	}
}
