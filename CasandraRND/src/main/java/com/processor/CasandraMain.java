package com.processor;

import com.db.DbTransactionService;

public class CasandraMain {

    public static void main(String[] args) {

        System.out.println("Main method started to communicate casandra");

        String hostName = "127.0.0.1";
        int portNumber =  9042;
        DbTransactionService dbTransactionService = new DbTransactionService();

        boolean status = dbTransactionService.createKeyspace(hostName,portNumber);

        System.out.println("Is created keyspace : "+status);

        System.out.println("Main method ended");

    }
}
