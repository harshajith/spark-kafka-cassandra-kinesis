package com.redmart.spark.kinesis.integ

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import java.util.Properties
import com.redmart.kafka.producer.EventPublisher
import collection.JavaConversions._
import org.json4s._
import org.json4s.native.JsonMethods._
import com.redmart.spark.kafka.integ.Helper
import com.redmart.spark.kafka.integ.Event
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kinesis._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient

/**
 * Consumes messages from one or more topics in Kafka
 * and calculates pick rate per user basis.
 */

object PickRateAnalyser {

    implicit val formats = DefaultFormats;

    def parser(json: String): Event = {
        val parsedJson = parse(json)
        return parsedJson.extract[Event] // Extract into the case class
    }

    def main(args: Array[String]) = {
        printf("--------------- starting spark consumer with kinesis -----------------" );

        val cassandraHosts = System.getenv("CASSANDRA_HOST_0") + "," + System.getenv("CASSANDRA_HOST_1")

        /* Determine the number of shards from the stream */
        val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
              kinesisClient.setEndpoint("kinesis.ap-southeast-1.amazonaws.com")
        val numShards = kinesisClient.describeStream("rm_pick_event_stream").getStreamDescription().getShards()
                  .size()

        val numStreams = numShards
        printf("test" + numStreams)
        val batchInterval = Seconds(5)
         /* Kinesis checkpoint interval.  Same as batchInterval for this example. */
        val kinesisCheckpointInterval = batchInterval


        val sparkConf = new SparkConf().setAppName("Pick-Rate-Analyser")
            .set("spark.cassandra.connection.host", cassandraHosts)
            .set("cassandra.connection.native.port", "9042")

        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, batchInterval)
        ssc.checkpoint("checkpoint")

        //val kafkaDStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_ONLY)

        val kinesisStreams = (0 until numStreams).map { i =>
          KinesisUtils.createStream(ssc, "rm_pick_event_stream", "https://kinesis.ap-southeast-1.amazonaws.com", batchInterval,
            InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2)
        }

        /* Union all the streams */
        val unionStreams = ssc.union(kinesisStreams)
        //unionStreams.print()

        val rdds = unionStreams.map(byteArray => new String(byteArray)).map(parser)
        rdds.print()


        rdds.saveToCassandra("redmart", "pick_events", SomeColumns("time_stamp", "user_id", "user_name", "session_id", "event_type", "item", "order_id", "tote_id", "quantity", "from_location",
            "from_location_zone", "from_warehouse", "order_line_num", "remarks", "trolley_id", "lot", "company")) // rdds.saveToCassendra event table.

        val logginStream = rdds.filter(s => s.eventType == "LoggedIn") // filter loggedIn events and update the latest loggedIn time
            .map(a => (a.userId, a))
            .updateStateByKey[Long](Helper.keepLoggedInTime) // return (user_id, latest_logged_time)

        val pickStream = rdds.filter(s => s.eventType == "PickConfirm" || s.eventType == "LoggedOut").map(s => (s.userId, s)) // filter only Pick stream and loggedOut stream and map into K,V

        val accumulatedStream = pickStream.updateStateByKey { (values: Seq[Event], state: Option[Seq[Event]]) =>

            val previousCount = state.getOrElse(null);
            val eventTypeSeq = values.map(a => a.eventType)
            if (eventTypeSeq.contains("LoggedOut")) {
                None
            } else {
                if (previousCount != null && values.length > 0) {
                    Some(previousCount ++ values)
                } else if (previousCount != null) {
                    Some(previousCount)
                } else if (values.length > 0) {
                    Some(values)
                } else {
                    null
                }
            }
        } // return (user_id, eventSeq)

        val eventCountStrem = accumulatedStream.map { case (user_id, eventSeq) => (user_id, eventSeq.length) } // get (user_id, event_count)

        val currentStream = eventCountStrem.join(logginStream) // return (user_id, (event_count, login_time)
        currentStream.print()

        val finalStream = currentStream.map {
            case (user_id, (event_count, login_time)) =>
                (Helper.getUUID, user_id, Helper.getRate(event_count, login_time), System.currentTimeMillis())
        } // return (user_id, pick_rate(per minute))

        finalStream.saveToCassandra("redmart", "all_pick_rate", SomeColumns("uid", "user_id", "pick_rate", "time_stamp"))

        val dashboardStream = finalStream.map { case (uid, user_id, pick_rate, time_stamp) => (user_id, pick_rate, time_stamp) }
        dashboardStream.cache()

        dashboardStream.saveToCassandra("redmart", "current_pick_rate", SomeColumns("user_id", "pick_rate", "time_stamp"))
        finalStream.print()

        val kafkaMsgs = dashboardStream.map {
            case (user_id, pick_rate, time_stamp) =>
                if (user_id != null) {
                    val kafkaPublisher = EventPublisher.getInstance();
                    var map = new java.util.HashMap[java.lang.String, Object]
                    map += helper("topicName", "dashboard_topic")
                    map += helper("partitionerKey", user_id)
                    map += helper("user_id", user_id)
                    map += helper("pick_rate", pick_rate)
                    map += helper("time_stamp", time_stamp)
                    kafkaPublisher.publishEvent(map)
                }
        }

        kafkaMsgs.print()

        ssc.start()
        ssc.awaitTermination()

        printf("----------------context is created RM -----------------------")

    }

    def sendToCassendra(events: List[Event]) = {
        for (event <- events) {
            println("userID : " + event.userId);
        }
    }

    def helper(s: String, a: Any) = s -> a.asInstanceOf[AnyRef]

}