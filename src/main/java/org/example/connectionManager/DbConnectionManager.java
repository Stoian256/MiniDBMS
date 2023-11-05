package org.example.connectionManager;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.Optional;

public class DbConnectionManager {
    private static final String CONNECTION_STRING = "mongodb+srv://mini_dbms_user:znyRpmoyRBZSdUum@cluster0.7wfpod0.mongodb.net/?retryWrites=true&w=majority";

    public static Optional<MongoDatabase> getMongoDatabase(String databaseName) {
        ServerApi serverApi = getServerApi();
        MongoClientSettings settings = getMongoClientSettings(serverApi);

        try (MongoClient mongoClient = MongoClients.create(settings)) {
            try {
                return Optional.of(mongoClient.getDatabase(databaseName));
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }
        return Optional.empty();
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
