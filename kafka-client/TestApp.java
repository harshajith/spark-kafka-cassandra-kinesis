import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.redmart.kafka.partitioner.PartitionerType;
import com.redmart.kafka.producer.EventPublisher;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestApp {

	private static String TOPIC_NAME = "page_visits";
	private static PartitionerType P_TYPE = PartitionerType.SIMPLE;
	private static String PICK_CONFIRM = "PickConfirm";
	private static String LOGGED_IN = "LoggedIn";
	private static String LOGGED_OUT = "LoggedOut";
	
	public static void main(String[] args) {
        long events = 10;
        Random rnd = new Random();
        
        EventPublisher eventPublisher = EventPublisher.getInstance();
        
        
        try {
        	Map<String, Object> event = new HashMap<String, Object>();
    		event.put("topicName", TOPIC_NAME);
    		event.put("sessionId", 34133L);
    		event.put("payload", "{}");

    		// ================= User login 1 ===============================
    		event.put("eventType", LOGGED_IN);
    		event.put("timeStamp", 1422769200000L);
    		event.put("userId", 1000L);
    		event.put("item", 2);
    		event.put("orderId", 2);
    		event.put("toteId", 3232423L);
    		event.put("quantity", 2);
    		event.put("fromLocation", "AC-3");
    		event.put("partitionerKey", event.get("userId"));
    		eventPublisher.publishEvent(event);
    		
    		
    		for (int i = 1; i <= 3; i++) {
    			event.put("timeStamp", System.currentTimeMillis()); // 1422639001322
    			event.put("eventType", PICK_CONFIRM);
    			eventPublisher.publishEvent(event);
    		}
    		
    		// ================= User 2 Login ===============================
    		event.put("eventType", LOGGED_IN);
    		event.put("timeStamp", 1422769200000L);
    		event.put("userId", 2000L);
    		event.put("partitionerKey", event.get("userId"));
    		eventPublisher.publishEvent(event);
    
    		// ================= User 2 : 30 pick events ================
    		for (int i = 1; i <= 5; i++) {
    			event.put("timeStamp", System.currentTimeMillis());
    			event.put("eventType", PICK_CONFIRM);
    			eventPublisher.publishEvent(event);
    		}
    		
    		System.out.println("Messages sent !");
    		
        }catch(Exception e){
        	e.printStackTrace();
        }
        
        
//        try {
//        	Properties props = new Properties();
//            props.put("metadata.broker.list", "ip-10-167-135-180:9092,54.179.26.160:9092");
//            props.put("serializer.class", "kafka.serializer.StringEncoder");
//           // props.put("partitioner.class", "com.redmart.kafka.partitioner.KeyBasedPartitioner");
//            props.put("request.required.acks", "1");
//     
//            ProducerConfig config = new ProducerConfig(props);
//     
//            Producer<String, String> producer = new Producer<String, String>(config);
//
//            for (long nEvents = 0; nEvents < events; nEvents++) { 
//                   long runtime = new Date().getTime();  
//                   String ip = "Test";
//                   String msg = ",www.example.com," + ip; 
//                   KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//                   producer.send(data);
//                   System.out.println("msg sent");
//            }
//            producer.close();
//        }catch(Exception e){
//        	e.printStackTrace();
//        }
 
        
    }
}
