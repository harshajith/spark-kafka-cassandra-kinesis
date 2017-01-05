package com.redmart.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 

/**
 * Based on the key it will decide which partition to be published, 
 * same key will always publish to same partition.
 * @author Harsha
 *
 */
public class KeyBasedPartitioner implements Partitioner {
	
    public KeyBasedPartitioner (VerifiableProperties props) {
    	
    }

    @Override
	public int partition(Object key, int numOfPartitions) {
		int partition = 0;
		if(key != null){
			Integer keyValue = Integer.valueOf(key.toString());
			partition = keyValue % numOfPartitions; 
			System.out.println("Partition to be published : " + partition);
		}
		return partition;
	}
 
}