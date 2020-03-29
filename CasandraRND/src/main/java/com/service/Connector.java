package com.service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Connector {


    public Cluster createCluster(String hostName, int port){

        Cluster cluster = Cluster.builder().addContactPoint(hostName).withPort(port).build();

        if(cluster == null){
            System.out.println("Cluster didn't initialized");
        }
        return cluster;

    }

    public Session createSession(Cluster cluster){
        Session session = null;
        if(cluster != null && !cluster.isClosed()){
            session = cluster.connect();
        }
        return session;
    }

    public void close(Cluster cluster, Session session){
        if(cluster != null && !cluster.isClosed()){
            cluster.close();
        }if(session != null && !session.isClosed()){
            session.close();
        }
        System.out.println("Cluster/Session are closed");
    }

}
