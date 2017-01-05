package com.redmart.spark.kafka.integ

/**
 * case class which maps pick stream events.
 */
case class Event(
    
    partitionerKey: Option[Int], 
    timeStamp: Long,
    userId: String, 
    userName: Option[String],
    sessionId : Option[String] , 
    eventType : String,
    item : Option[String],
    lot: Option[String],
    orderId: Option[String],
    toteId: Option[String],
    quantity: Option[Int],
    fromLocation: Option[String],
    company: Option[String],
    fromLocationZone: Option[String],
    fromWarehouse: Option[String],
    orderLineNum: Option[String],
    trolleyId: Option[String],
    remarks: Option[String]
    
    
    ) extends Serializable(
)