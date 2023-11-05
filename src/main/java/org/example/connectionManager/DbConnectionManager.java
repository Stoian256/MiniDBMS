package org.example.connectionManager;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.Optional;

public class DbConnectionManager {
    private static final String CONNECTION_STRING = "mongodb+srv://mini_dbms_user:znyRpmoyRBZSdUum@cluster0.7wfpod0.mongodb.net/?retryWrites=true&w=majority";

    public static MongoClient getMongoClient() {
        ServerApi serverApi = getServerApi();
        MongoClientSettings settings = getMongoClientSettings(serverApi);
        return MongoClients.create(settings);
    }

    private static ServerApi getServerApi() {
        return ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();
    }

    private static MongoClientSettings getMongoClientSettings(ServerApi serverApi) {
        return MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(CONNECTION_STRING))
                .serverApi(serverApi)
                .build();
    }
}
