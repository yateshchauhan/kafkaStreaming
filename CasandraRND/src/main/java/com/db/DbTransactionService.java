package com.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.service.Connector;

public class DbTransactionService {

    public boolean createKeyspace(String hostName, int port){

        boolean status = false;
        String cql = "create keyspace NewDB with replication = {'class':'SimpleStrategy', 'replication_factor':1}";

        Connector connector = new Connector();
        Cluster cluster = connector.createCluster(hostName,port);
        Session session = connector.createSession(cluster);

        try {
            ResultSet resultSet = session.execute(cql);

            status = resultSet != null ? resultSet.wasApplied() : status;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            connector.close(cluster, session);
            
        }

        return status;
    }
}
