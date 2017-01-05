package com.redmart.kafka.producer.test;

import java.util.HashMap;
import java.util.Map;

import com.redmart.kafka.partitioner.PartitionerType;
import com.redmart.kafka.producer.EventPublisher;


/**
 * Test class to publish events to Kafka
 * @author Harsha
 *
 */
public class ProducerTest {

	private static String TOPIC_NAME = "page_visits";
	private static PartitionerType P_TYPE = PartitionerType.SIMPLE;
	private static String PICK_CONFIRM = "PickConfirm";
	private static String LOGGED_IN = "LoggedIn";
	private static String LOGGED_OUT = "LoggedOut";

	public static void main(String[] args) {

		System.out.println(System.currentTimeMillis());
		Map<String, Object> event = new HashMap<String, Object>();
		event.put("topicName", TOPIC_NAME);
		event.put("sessionId", 34133L);
		event.put("payload", "{}");

		// ================= User login 1 ===============================
		EventPublisher eventPublisher = EventPublisher.getInstance();
		event.put("eventType", LOGGED_IN);
		event.put("timeStamp", 1423379700000L);
		event.put("userId", 1000L);
		event.put("item", 2);
		event.put("orderId", 2);
		event.put("toteId", 3232423L);
		event.put("quantity", 2);
		event.put("fromLocation", "AC-3");
		event.put("partitionerKey", event.get("userId"));
//		eventPublisher.publishEvent(event);
//
//		// ================= User 1 : 3 pick events ================
//		for (int i = 1; i <= 100; i++) {
//			event.put("timeStamp", System.currentTimeMillis()); // 1422639001322
//			event.put("eventType", PICK_CONFIRM);
//			eventPublisher.publishEvent(event);
//
//		}
		
		event.put("timeStamp", System.currentTimeMillis()); // 1422639001322
		event.put("eventType", LOGGED_OUT);
		eventPublisher.publishEvent(event);


	}

}
