package com.redmart.cassandra.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient {
   private Cluster cluster;
   private Session session;
   

   public void connect(String node) {
      cluster = Cluster.builder()
            .addContactPoint(node)
            .build();
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
   }

   public void close() {
      cluster.close();
   }

   public static void main(String[] args) {
      SimpleClient client = new SimpleClient();
      client.connect("54.179.26.160");
      client.close();
   }
}