package com.redmart.spark.kafka.integ

import java.util.UUID

import org.apache.spark.sql.cassandra.CassandraSQLContext

import kafka.producer.KeyedMessage

object Helper {

    /**
     * This will keep the previous state and update with the new values.
     * It will update the current day event counts.
     *
     */
    val updateFunc1 = (values: Seq[Event], state: Option[Seq[Event]]) => {

        //val currentCount = values.length
        val previousCount = state.getOrElse(null)
        var aggregate: Seq[Event] = null;
        if (previousCount != null) {
            aggregate = (values ++ previousCount)
        } else {
            aggregate = values
        }
        Some(aggregate)
    }

    val updateFunc = (values: Seq[(Long, Event)], state: Option[Seq[Event]]) => {

        //val currentCount = values.length
        var eventsToFilter: Seq[Event] = null;
        val previousCount = state.getOrElse(null);
        var lastLoginTime = 0L;
        var currentEvents: Seq[Event] = null;

        if (values.length > 0) {
            currentEvents = values.map(a => a._2);
            lastLoginTime = values.map(a => a._1).max
        }

        if (previousCount != null && currentEvents != null && currentEvents.length > 0) {
            eventsToFilter = currentEvents ++ previousCount
        } else if (currentEvents != null && currentEvents.length > 0) {
            eventsToFilter = currentEvents;
        } else if (previousCount != null)
            eventsToFilter = previousCount;

        val filteredEvents = eventsToFilter.filter(a => a.timeStamp > lastLoginTime)
        Some(filteredEvents)

    }

    val updateFuncNew = (values: Seq[((Long, Event), Long)], state: Option[Seq[Event]]) => {
        if (values.length > 0) {
            None;
        } else {
            Some(values.map(a => a._1._2))
        }
    }

    val filterLogOuts = (userId: Int, logedOutUsers: Seq[Int]) => {
        if (logedOutUsers.length > 0) {
            if (logedOutUsers.contains(userId)) {
                false;
            } else {
                true;
            }
        }

    }
    /**
     * get the last login time
     */
    val keepLoggedInTime = (values: Seq[Event], state: Option[Long]) => {

        if (values.length > 0) {
            val latestLoggedTime = values.map(a => a.timeStamp).max
            Some(latestLoggedTime)

        } else {
            Some(state.getOrElse(System.currentTimeMillis()))
        }

    }

    /**
     * Get the pick rate per minute
     */
    val getRate = (event_counts: Double, login_time: Long) => {
        if (event_counts <= 0) {
            0.000;
        } else {
            val pickRate = event_counts * 60000 / (System.currentTimeMillis() - login_time)
            if (pickRate < 0) {
                0.000;
            } else {
                pickRate
            }
        }
    }

    val compareEvents = (values: Seq[Event], lastLoginTime: Long) => {

        val currentEvents = values.filter { a => a.timeStamp > lastLoginTime }
        currentEvents.length

    }

    val getReadableDate = (timeInMillis: Long) => {
        timeInMillis
    }
    /**
     * Test for deleting a record.
     */
    val deleteRecord = (event: Event, cc: CassandraSQLContext) => {
        cc.setKeyspace("redmart")
        val rdd = cc.cassandraSql("SELECT user_id, event_type FROM user_login")
        rdd.collect().foreach(println) // [2, 4.0] [1, 10.0]
    }


    /**
     * Calculates the actual pick rate
     */
    def getPickRate(event_counts: Double, loginTime: Long, logOutTime: Long): Double = {
        if (event_counts <= 0) {
            0.000;
        } else {
            val pickRate = event_counts * 60000 / (logOutTime - loginTime)
            if (pickRate < 0) {
                0.000;
            } else {
                pickRate
            }
        }
    }

    def sendMsg(msg: KeyedMessage[String, String]): Unit = {

    }
    /**
     * Generate a UUID for
     */
    def getUUID(): UUID = {
        java.util.UUID.randomUUID
    }

}