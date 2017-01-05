package com.redmart.kafka.partitioner;

/**
 * Define specific partitioner types to be used 
 * while publishing messages
 * @author Harsha
 *
 */
public enum PartitionerType {
	
	SIMPLE("com.redmart.kafka.partitioner.KeyBasedPartitioner"),
	CUSTOM("com.redmart.kafka.partitioner.Custom");

    private String partitionerClass;
        
    PartitionerType(String pClass){
    	this.partitionerClass = pClass;
    }

	public String getPartitionerClass() {
		return partitionerClass;
	}

	public void setPartitionerClass(String partitionerClass) {
		this.partitionerClass = partitionerClass;
	}

}
