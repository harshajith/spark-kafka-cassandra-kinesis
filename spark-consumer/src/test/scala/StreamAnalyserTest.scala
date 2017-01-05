package com.redmart.spark.kafka.integ

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s._
import org.json4s.native.JsonMethods._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */

object StreamAnalyserTest {

  implicit val formats = DefaultFormats

  // ================= GET VALUES WITHOUT CASE CLASS ===========================

  //  def parser(json: String): List[JValue] = {
  //    val parsedJson = parse(json)
  //    val a = (parsedJson \\ "userId")
  //    val b = (parsedJson \\ "partitionerKey")
  //    return List(a,b)
  //  }

  def parser(json: String): Event = {
    val parsedJson = parse(json)
    return parsedJson.extract[Event] // Extract into the case class
  }

  def main(args: Array[String]) = {
    printf("--------------- starting spark consumer -----------------");

    val zkQuorum = "localhost:2181"
    val group = "consumer-group";
    val topics = "page_visits";
    val numThreads = 1;

     

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("cassandra.connection.native.port", "9042")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaDStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_ONLY)
    //kafkaDStream.print()

    val rdds = kafkaDStream.map(_._2).map(parser)
    rdds.persist()
    
    val logginEvents = rdds.filter(s => s.eventType == "LoggedIn")
    logginEvents.saveToCassandra("redmart", "user_login", SomeColumns("user_id", "timeStamp", "eventType"))
    
    
    val pairs = rdds.filter(s => s.eventType == "PickConfirm").map(s => (s.userId, s))
    //val stateDstream = pairs.updateStateByKey[Double](DataAnalyser.updateFunc)

   // stateDstream.print()
    //stateDstream.saveToCassandra("redmart", "users", SomeColumns("user_id", "pick_count"))
    //val runningCounts = pairs.updateStateByKey[Int](updateFunction _)

    //    val countsRDD = pairs.reduceByKey((a, b) => a + b)
    //    countsRDD.print()
    //    
    //    countsRDD.saveToCassandra("redmart", "users", SomeColumns("user_id", "pick_count"))
    //    
    //    val rdd = ssc.cassandraTable("redmart", "users").select("user_id", "pick_count")
    
            //join.print()
    
    //logginStream.updateStateByKey[Seq[(Long, Event)]](updateFunc)
    
    // check if the event timestamp is > loggedIn time and convert dataset back to to (user_id, event)
    //val currentEventStream = initialJoin.filter{case (x,(y,z)) => (z.timeStamp > y)}.map{case (x,(y,z)) => (x,z)} 
    //initialJoin.print()

    //=========================
//    val initialJoin = accumulatedStream.join(logginStream) // return user_id, (seq[events], login_time)
//    initialJoin.print()
//    
//    val stream1 = initialJoin.map{case (user_id, (eventsSeq, loginTime)) => (user_id, (DataAnalyser.compareEvents(eventsSeq, loginTime), loginTime))}   //// return (user_id, (event_counts, logged_in_time))
//
//    //val joinedStream = accumulatedStream.join(logginStream) 
//    val finalStream = stream1.map{case (user_id,(event_counts,logged_time)) => (user_id, DataAnalyser.getRate(event_counts,logged_time))} // return (user_id, pick_rate(per minute))
//
//
//    finalStream.saveToCassandra("redmart", "user_pick_rate", SomeColumns("user_id", "pick_rate"))
//    finalStream.print()

    //==========================
    
    //val logoutEvents = rdds.filter(s => s.eventType == "LoggedOut").map(deleteUserRecord)
    //logginEvents.saveToCassandra("redmart", "user_login", SomeColumns("user_id", "time_stamp", "event_type"))
    //stateDstream.saveToCassandra("redmart", "users", SomeColumns("user_id", "pick_count"))

    //val runningCounts = pairs.updateStateByKey[Int](updateFunction _)

    //    val countsRDD = pairs.reduceByKey((a, b) => a + b)
    //    countsRDD.print()
    //    
    //    countsRDD.saveToCassandra("redmart", "users", SomeColumns("user_id", "pick_count"))
    //    
    //    val rdd = ssc.cassandraTable("redmart", "users").select("user_id", "pick_count")

    ssc.checkpoint("/Users/Harsha/spark-consumer/checkpoint")
    ssc.start()
    ssc.awaitTermination()

    printf("----------------context is created RM -----------------------")

  }

  def sendToCassendra(events: List[Event]) = {
    for (event <- events) {
      println("userID : " + event.userId);
    }

  }
  
//  def saveLoginUser(event : Event) : Unit = {
//    println("================= :" + event.userId)
//    println("================= :" + event.)
//  }

}