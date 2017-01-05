package com.redmart.spark.kafka.integ

case class Event(
    
    partitionerKey:Int, 
    timeStamp: Long,
    userId: Int, 
    sessionId : Int , 
    eventType : String
    
    
    
)