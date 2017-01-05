package com.redmart.cassandra.client;

import com.datastax.driver.core.*;

public class CassandraClient {
 
 
	public static void main(String[] args) {
 
		Cluster cluster;
		Session session;
		
		try {

			// Connect to the cluster and keyspace "demo"
			cluster = Cluster.builder().addContactPoint("54.151.137.92").addContactPoint("54.254.3.15").withPort(9042).build();
			session = cluster.connect("redmart");

	 
			// Use select to get the user we just entered
			ResultSet results = session.execute("SELECT * FROM current_pick_rate");
			for (Row row : results) {
					System.out.println( "user_id : " + row.getInt("user_id") + " pick_rate : " + row.getDouble("pick_rate"));
			}
	 

			// Clean up the connection by closing it
		    cluster.close();
		    
		}catch (Exception e){
			e.printStackTrace();
		}
 
	}
}