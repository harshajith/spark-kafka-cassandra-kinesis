package com.redmart.kafka.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.redmart.kafka.partitioner.PartitionerType;

/**
 * Simple Kafka client to publish events.
 * Brokers are set with KAFKA_BROKERS ENV variable in "host:port, host1:port1"
 * @author Harsha
 *
 */
public class EventPublisher {
	
	//private static String brokerUrl=   // "localhost:9092, localhost:9093"; //"54.251.10.170:9092, 54.251.10.170:9093"
	Logger logger = LoggerFactory.getLogger(EventPublisher.class);
	
	private static final String DEFAULT_TOPIC = "default_topic";
	private static final String DEFAULT_EVENT = "default_event";
	private static final Integer DEFAULT_KEY = 100;
	
	private static Map<String, EventPublisher> publisherMap = new HashMap<String, EventPublisher>();
	private static EventPublisher eventPublisher = null;
	private static Gson gson = new Gson();
	
	public Producer<String, String> producer;
	private String topicName;
	private String eventType;
	
	/**
	 * @param topicName
	 * @param pType
	 * @param eventType
	 * @param customPartitioner
	 */
	private EventPublisher(String topicName, PartitionerType pType, String eventType, String customPartitioner){
		
		Properties props = new Properties();
		props.put("metadata.broker.list", System.getenv("KAFKA_BROKERS"));
		props.put("request.required.acks", "0"); // producer does wait for ack.
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		if(pType == PartitionerType.SIMPLE || pType == null){
			props.put("partitioner.class", PartitionerType.SIMPLE.getPartitionerClass());
		
		}else if(pType == PartitionerType.CUSTOM)  {
			if(customPartitioner != null && !customPartitioner.isEmpty()){
				props.put("partitioner.class", customPartitioner);
			}
		}
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		
		if(topicName != null && !topicName.isEmpty()){
			this.setTopicName(topicName);
		}else {
			this.setTopicName(DEFAULT_TOPIC);
		}
		
		if(eventType != null && !eventType.isEmpty()){
			this.setEventType(eventType);
		}else {
			this.setEventType(DEFAULT_EVENT);
		}

		
	}
	
	
	
	
	/**
	 * 
	 * @param topicName
	 * @param pType
	 * @param eventType
	 * @param customPartitioner
	 */
	private EventPublisher(String topicName){
		
		Properties props = new Properties();
		props.put("metadata.broker.list", System.getenv("KAFKA_BROKERS"));
		props.put("request.required.acks", "0"); // producer does not wait for ack.
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		
		if(topicName != null && !topicName.isEmpty()){
			this.setTopicName(topicName);
		}else {
			this.setTopicName(DEFAULT_TOPIC);
		}
		
		logger.info("Obtains a publisher for the KAFKA cluster : " + System.getenv("KAFKA_BROKERS"));
		
	}
	
	/**
	 * 
	 * @param topicName
	 * @param pType
	 * @param eventType
	 * @param customPartitioner
	 */
	private EventPublisher(){
		
		Properties props = new Properties();
		props.put("metadata.broker.list", System.getenv("KAFKA_BROKERS"));
		props.put("request.required.acks", "0"); // producer does not wait for ack.
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		
		logger.info("Obtains a publisher for the KAFKA cluster : " + System.getenv("KAFKA_BROKERS"));

	}


	/**
	 * Publish event to the given topic in the hashmap.
	 * Further it uses partitionerKey property to distribute messages.
	 * @param event
	 */
	public void publishEvent(Map<String, Object> event){
		try{
			
			event.put("instanceId", System.getenv("INSTANCE_ID")); 
			String topicName = event.get("topicName") != null ? event.get("topicName").toString() : "DEFAULT_TOPIC";
			String partitionerKey = event.get("partitionerKey") != null ? event.get("partitionerKey").toString() : String.valueOf(DEFAULT_KEY); // get the has of the key to publish to paritions.
			
			String jsonStr = gson.toJson(event);
			logger.info("Event to be published : topic : " + event.get("topicName") + " :: json : " + jsonStr);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, partitionerKey, jsonStr);
			producer.send(data);
		
		}catch(Exception e){
			logger.error("Exception occurred while publishing to the topic : " + e.toString());
		}
		
	}

	
	/**
	 * This will return an publisher instance with it's own configuration based on event type.
	 * @param topicName
	 * @param partitionerType
	 * @param eventType
	 * @param customPartitioner
	 * @return
	 */
	public static EventPublisher getInstance(String topicName, PartitionerType partitionerType, String eventType, String customPartitioner){
		
		EventPublisher eventPublisher = publisherMap.get(eventType);
		
		if(eventPublisher == null){
			eventPublisher = new EventPublisher(topicName, partitionerType, eventType, customPartitioner);
			publisherMap.put(eventType, eventPublisher);
		}
		
		return eventPublisher;
	}
	
	/**
	 * This will return an publisher instance with it's own configuration based on event types.
	 * 
	 * @param topicName
	 * @param partitionerType
	 * @param eventType
	 * @param customPartitioner
	 * @return
	 */
	public static EventPublisher getInstance(){
		
		if(eventPublisher == null){
			eventPublisher = new EventPublisher();
		}
		
		return eventPublisher;
	}
	
	

	public String getTopicName() {
		return topicName;
	}


	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	
	
	


}
