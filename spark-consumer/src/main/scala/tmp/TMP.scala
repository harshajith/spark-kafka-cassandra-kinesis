//package com.redmart.spark.kafka.integ
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.StreamingContext._
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.json4s._
//
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.streaming._
//import java.util.Properties
//import com.redmart.kafka.producer.EventPublisher
//import collection.JavaConversions._
//
//import org.json4s.native.JsonMethods._
//
///**
// * Consumes messages from one or more topics in Kafka
// * and calculates pick rate per user basis.
// */
//
//object StreamAnalyser {
//
//    implicit val formats = DefaultFormats;
//
//    def parser(json: String): Event = {
//        val parsedJson = parse(json)
//        return parsedJson.extract[Event] // Extract into the case class
//    }
//
//    def main(args: Array[String]) = {
//        val brokers = System.getenv("KAFKA_BROKERS")
//        printf("--------------- starting spark consumer -----------------" + brokers);
//
//        // Sending the results back to a KAFKA topic
//        // Zookeper connection properties
//        
//        val props = new Properties()
//        props.put("metadata.broker.list", brokers)
//        props.put("serializer.class", "kafka.serializer.StringEncoder")
//
//        val zkQuorum = System.getenv("ZOOKEEPER_HOST"); //args(0);
//        val group = "spark_cluster"; //args(1);
//        val topics = "page_visits";
//        val numThreads = 2;
//
//        val cassandraHosts = System.getenv("CASSANDRA_HOST_0") + "," + System.getenv("CASSANDRA_HOST_1")
//
//        val sparkConf = new SparkConf().setAppName("Pick-Rate-Analyser")
//            .set("spark.cassandra.connection.host", cassandraHosts)
//            .set("cassandra.connection.native.port", "9042")
//
//        val sc = new SparkContext(sparkConf)
//        val ssc = new StreamingContext(sc, Seconds(30))
// 
//
//        ssc.checkpoint("checkpoint")
//        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//        val kafkaDStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_ONLY)
//        //kafkaDStream.print()
//
//        val rdds = kafkaDStream.map(_._2).map(parser)
//        rdds.persist()
//        rdds.saveToCassandra("redmart", "pick_events", SomeColumns("time_stamp", "user_id", "user_name", "session_id", "event_type", "item", "order_id", "tote_id", "quantity", "from_location", 
//                "from_location_zone","from_warehouse","order_line_num","remarks","trolley_id", "lot", "company")) // rdds.saveToCassendra event table.
//
//        val logginStream = rdds.filter(s => s.eventType == "LoggedIn") // filter loggedIn events and update the latest loggedIn time
//            .map(a => (a.userId, a))
//            .updateStateByKey[Long](DataAnalyser.keepLoggedInTime) // return (user_id, latest_logged_time)
//
//        val pickStream = rdds.filter(s => s.eventType == "PickConfirm" || s.eventType == "LoggedOut").map(s => (s.userId, s)) // filter only Pick stream and loggedOut stream and map into K,V
//        val join = logginStream.join(pickStream) // (user_id, (login_time, event))
//
//        val stream1 = pickStream.updateStateByKey { (values: Seq[Event], state: Option[Seq[Event]]) =>
//
//            val previousCount = state.getOrElse(null);
//            val eventTypeSeq = values.map(a => a.eventType)
//            if (eventTypeSeq.contains("LoggedOut")) {
//                None
//            } else {
//                if (previousCount != null && values.length > 0) {
//                    Some(previousCount ++ values)
//                } else if (previousCount != null) {
//                    Some(previousCount)
//                } else if (values.length > 0) {
//                    Some(values)
//                } else {
//                    null
//                }
//            }
//        } // return (user_id, eventSeq)
//
//        //    val filteredStream = join.updateStateByKey[Seq[Event]](DataAnalyser.updateFunc) // return (user_id, seq[events]), keep the current event_counts, filter older events
//        //    filteredStream.print()
//
//        val eventCountStrem = stream1.map { case (user_id, eventSeq) => (user_id, eventSeq.length) } // get (user_id, event_count)
//
//        val currentStream = eventCountStrem.join(logginStream) // return (user_id, (event_count, login_time)
//        currentStream.print()
//
//        val finalStream = currentStream.map { case (user_id, (event_count, login_time)) => (DataAnalyser.getUUID, user_id, DataAnalyser.getRate(event_count, login_time), System.currentTimeMillis()) } // return (user_id, pick_rate(per minute))
//
//        finalStream.saveToCassandra("redmart", "all_pick_rate", SomeColumns("uid", "user_id", "pick_rate", "time_stamp"))
//        val dashboardStream = finalStream.map { case (a, b, c, d) => (b, c, d) }
//        dashboardStream.saveToCassandra("redmart", "current_pick_rate", SomeColumns("user_id", "pick_rate", "time_stamp"))
//        finalStream.print()
//
//        val kafkaMsgs = dashboardStream.map { case (a, b, c) =>
//            if(a != null) {
//                val kafkaPublisher = EventPublisher.getInstance();
//                var map = new java.util.HashMap[java.lang.String, Object]
//                map += helper("topicName", "dashboard_topic")
//                map += helper("partitionerKey", "100")
//                map += helper("user_id", a)
//                map += helper("pick_rate", b)
//                map += helper("time_stamp", c)
//                kafkaPublisher.publishEvent(map)
//            }
//        }
//        
//        kafkaMsgs.print()
//
//        ssc.start()
//        ssc.awaitTermination()
//
//        printf("----------------context is created RM -----------------------")
//
//    }
//
//    def sendToCassendra(events: List[Event]) = {
//        for (event <- events) {
//            println("userID : " + event.userId);
//        }
//
//    }
//    
//    def helper(s: String, a: Any) = s -> a.asInstanceOf[AnyRef]
//
//}