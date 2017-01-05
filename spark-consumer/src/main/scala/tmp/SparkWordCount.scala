/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tmp

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.json4s._
import org.json4s.native.JsonMethods._

object SparkWordCount {
  def main(args: Array[String]) {
    
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val threshold = args(1).toInt
    
    // split each document into words
    val lines = sc.textFile(args(0)).flatMap(_.split(" "))
    
    //val userId = lines.map(s=> s).map(parser)
    
    //implicit val formats = DefaultFormats;
    case class UserId(userId: Int)
    
//    def parser(json: String): Int = {
//        val parsedJson = parse(json)
//        val m = parsedJson.extract[UserId]
//        return m.userId
//    }
    
      // ================= GET VALUES WITHOUT CASE CLASS ===========================

  //  def parser(json: String): List[JValue] = {
  //    val parsedJson = parse(json)
  //    val a = (parsedJson \\ "userId")
  //    val b = (parsedJson \\ "partitionerKey")
  //    return List(a,b)
  //  }
    
    val json = parse("""
         { "name": "joe",
           "children": [
             {
               "name": "Mary",
               "age": 5
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
       """)
       
   
    
    val ageSum = null;
    
    def sumAge : List[BigInt] = {
       for {
         JObject(child) <- json
         JField("age", JInt(age))  <- child
         
       } yield age;
    }
   
    val ageList = sumAge;
    val agetotal = ageList.reduce((a,b) => a+b)
    // ageSum.map()
    print("---------------- :" + agetotal)
    
    
    // count the occurrence of each word
//    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
//    
//    // filter out words with less than threshold occurrences
//    val filtered = wordCounts.filter(_._2 >= threshold)
//    
//    // count characters
//    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
//    
//    System.out.println("Harsha you rock :" + charCounts.collect().mkString(", "))
  }
}
