package tmp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

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

object Test {
  def main(args: Array[String]) = {
    printf("--------------- starting spark consumer -----------------");

    val zkQuorum = args(0);
    val group = args(1);
    val topics = args(2);
    val numThreads = args(3);

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaDStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    printf("********************** : lines received ============== \n")
    kafkaDStream.print()
    //val words = lines.flatMap(_.split(","))
    //val wordCounts = words.map(x => (x, 1L))

    printf("---------------------------- ******** : SON")

//    val valueStream = kafkaDStream.map(
//      s => {
//        val json = new JsonWrapper
//        val js = json.parse(s)
//        val a = (js \ "partitionerKey").toString
//        val b = (js \ "timeStamp").toString
//        val c = (js \ "userId").toString
//        (a, b, c)
//
//      })
//      
//      valueStream.print()

    printf("---------------------------- ******** : BON")

    //    val words = lines.flatMap(_.split(" "))
    //    print("hello" + words)
    //    val wordCounts = words.map(x => (x, 1L))
    //      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    //    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

    printf("----------------context is created RM -----------------------")

    
    //val logginEvents = rdds.filter(s => s.eventType == "LoggedIn").map(saveLoginUser)
  }
}