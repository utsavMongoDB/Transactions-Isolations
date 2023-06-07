package com.example.demo.api.snapshot;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import java.util.HashMap;

@Component
@Service
public class RetryMechanism {
    public final String mongodbUri;
    static MongoClient mongoClient;
    static MongoDatabase db;
    static MongoCollection<Document> accountCollection;
    static HashMap<String, Integer> retryMap = new HashMap<>();
    static HashMap<String, Object[]> status = new HashMap<>();

    @Autowired
    public RetryMechanism(@Value("${uri}") String mongodbUri) {
        this.mongodbUri = mongodbUri;
        ConnectionString connectionString = new ConnectionString(this.mongodbUri);
        mongoClient = MongoClients.create(connectionString);
        db = mongoClient.getDatabase("AbcBank");
        accountCollection = db.getCollection("account");
    }

    public ResponseEntity<Object> performRetryTransaction(Integer id, String sessionId) {
        retryMechanism(id, sessionId);
        Object[] responseArray = status.get(sessionId);
        return ResponseEntity.status((Integer) responseArray[0]).body(responseArray[1]);
    }

    private void retryMechanism (Integer id, String sessionId) {
        String productId = getId(id);
        Double amount = 100.5;
        Integer RETRY_COUNT = 10;


        ClientSession clientSession = mongoClient.startSession();
        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.SNAPSHOT)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        TransactionBody<String> txnBody = () -> {
            UpdateResult updateResponse = accountCollection.updateOne(clientSession,
                    Filters.eq("_id", productId),
                    Updates.inc("availableBalance", amount)
            );
            System.out.println("response :"+ updateResponse.getModifiedCount());
            return "updated for "+productId;
        };

        try {
            // Start session and create the transaction
            clientSession.withTransaction(txnBody, txnOptions);
            // Response
            Object[] responseObj = new Object[2];
            responseObj[0] = 200;
            responseObj[1] = "Updated for " + sessionId;
            status.put(sessionId, responseObj);

        } catch (Exception e) {
            int retryCount = updateRetry(sessionId);
            System.out.println("Retry Count :" + sessionId + " :" + retryCount);
            if (retryCount <= RETRY_COUNT) {
                ++retryCount;
                retryMechanism(id, sessionId);
            } else {
                Object[] responseObj = new Object[2];
                responseObj[0] = 500;
                responseObj[1] = "Error updating :" + e.getMessage();
                status.put(sessionId, responseObj);
            }
        }
    }

    private int updateRetry(String sessionId) {
        int updatedValue = retryMap.containsKey(sessionId) ? retryMap.get(sessionId) + 1 : 1;
        retryMap.put(sessionId, updatedValue);
        return updatedValue;
    }

    private String getId(Integer pos) {
        String id;
        Document document = accountCollection.find().skip(pos - 1).limit(1).first();
        if (document == null) {
            // Get last element in case no document found
            document = accountCollection.find().sort(new Document("_id", -1)).limit(1).first();
        }
        id = (String) document.get("_id");
        System.out.println("ID selected -- " + id);
        return id;
    }
}
