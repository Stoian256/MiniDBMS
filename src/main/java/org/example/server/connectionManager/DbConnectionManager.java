package org.example.server.connectionManager;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DbConnectionManager {
    private static final String CONNECTION_STRING = "mongodb://localhost:27017";

    public static MongoClient getMongoClient() {
        ConnectionString connString = new ConnectionString(CONNECTION_STRING);
        return MongoClients.create(connString);
    }
}
