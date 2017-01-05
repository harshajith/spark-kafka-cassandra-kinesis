package controllers;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.redmart.kafka.partitioner.PartitionerType;
import com.redmart.kafka.producer.EventPublisher;
import com.redmart.rabbitmq.integ.RabbitMQPublisher;

import play.*;
import play.mvc.*;
import views.html.*;

public class Application extends Controller {

	private static String TOPIC_NAME = "page_visits";
	private static PartitionerType P_TYPE = PartitionerType.SIMPLE;
	private static String PICK_CONFIRM = "PickConfirm";
	private static String LOGGED_IN = "LoggedIn";
	private static String LOGGED_OUT = "LoggedOut";

	private static Logger logger = LoggerFactory.getLogger(Application.class);
	
	private static Cluster cluster;
	private static Session session;

	public static Result index() {

		try {

			EventPublisher eventPublisher = EventPublisher.getInstance();
			Map<String, Object> event = new HashMap<String, Object>();
			event.put("topicName", TOPIC_NAME);
			event.put("sessionId", 34133L);
			event.put("payload", "{}");
			event.put("quantity", 3);
			event.put("eventType", LOGGED_IN);
    		event.put("timeStamp", 1423467600000L);
    		event.put("userId", 5560L);
    		event.put("item", 2);
    		event.put("orderId", 2);
    		event.put("toteId", 3232423L);
    		event.put("lot", "");
    		event.put("company", "redmart");
    		event.put("fromLocationZone", "redmart");
    		event.put("fromWarehouse", "redmart");
    		event.put("orderLineNum", "21");
    		event.put("trolleyId", "32");
    		event.put("remarks", "remarks");
    		event.put("userName", "username");
    		event.put("company", "redmart");
    		event.put("fromLocation", "AC-3");
    		event.put("partitionerKey", event.get("userId"));
    		eventPublisher.publishEvent(event);

    		for (int i = 1; i <= 10; i++) {
    			event.put("timeStamp", System.currentTimeMillis()); // 1422639001322
    			event.put("eventType", PICK_CONFIRM);
    			eventPublisher.publishEvent(event);
    		}

    		
//    		event.put("timeStamp", System.currentTimeMillis()); // 1422639001322
//			event.put("eventType", LOGGED_OUT);
			
//			eventPublisher.publishEvent(event);
    		logger.debug("Event Published To KAFAK Cluster Successfully !");

		} catch (Exception e) {
			logger.error("Exception occurred while publishing events : " + e.toString());
		}

		return ok("Events Published");


	}
	
	
	public static Result sendMessageToQ() {
		
		RabbitMQPublisher publisher = RabbitMQPublisher.getInstance();
		try {
			publisher.publishMessage("TestQ-exchange", "TestQ-key", "Test message");
		} catch (Exception e) {
			logger.info("Error occurred while publishing messages : " + e.toString());
			e.printStackTrace();
		}
		
		return ok("Events Published");
	}
	
	
	
	public static Result readFromCassandra(){
		

		
		try {

			// Connect to the cluster and keyspace "demo"
			SocketOptions options = new SocketOptions();
			options.setConnectTimeoutMillis(10000);
			options.setReadTimeoutMillis(10000);
			cluster = Cluster.builder().addContactPoint("54.179.26.160").withSocketOptions(options).withPort(9042).build();
			 Metadata metadata = cluster.getMetadata();
		      System.out.printf("Connected to cluster: %s\n", 
		            metadata.getClusterName());
		      for ( Host host : metadata.getAllHosts() ) {
		         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
		               host.getDatacenter(), host.getAddress(), host.getRack());
		      }
			session = cluster.connect();

			// Use select to get the user we just entered
			ResultSet results = session.execute("SELECT * FROM redmart.user_pick_rate");
			for (Row row : results) {
				System.out.println( "user_id : " + row.getInt("user_id") + " pick_rate : " + row.getDouble("pick_rate"));
			}

			// Clean up the connection by closing it
		    cluster.close();
		    
		}catch (Exception e){
			e.printStackTrace();
		}
		
		return ok("Successfully returned from cassandra");
		
	}
	
	
	

}
