package com.example.demo.api.snapshot;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.example.demo.api.Helper.create_trx_options;
import static com.mongodb.client.model.Filters.eq;

@Service
public class QueueWaitMechanism {
    static MongoClient mongoClient;
    static MongoDatabase db;
    static MongoCollection<Document> accountCollection;
    public final String mongodbUri;
    // Define a blocking queue to hold the requests
    private BlockingQueue<Request> requestQueue = new LinkedBlockingQueue<>();

    @Autowired
    public QueueWaitMechanism(@Value("${uri}") String mongodbUri) {
        this.mongodbUri = mongodbUri;
        ConnectionString connectionString = new ConnectionString(this.mongodbUri);
        mongoClient = MongoClients.create(connectionString);
        db = mongoClient.getDatabase("AbcBank");
        accountCollection = db.getCollection("account");
    }

    public synchronized ResponseEntity<String> performTransactionWithQueue(Integer pos, Integer amount, String sessionId) throws InterruptedException {
        String productId = getId(pos);
        Integer disbursedAmount = amount;

        while (isLocked(productId)) {
            // If the document is locked, enqueue the request and wait until it's unlocked
            System.out.println("~~ Doc locked ~~" + productId);
            Request request = new Request(pos, sessionId);
            requestQueue.add(request);
            wait();
        }

        // Acquire lock on the document
        documentLock(productId, true);

        TransactionOptions txnOptions = create_trx_options(ReadConcern.SNAPSHOT);

        ClientSession clientSession = mongoClient.startSession();

        TransactionBody<String> txnBody = () -> {
            UpdateResult updateResponse = accountCollection.updateOne(clientSession,
                    Filters.eq("_id", productId),
                    Updates.inc("disbursedAmount", disbursedAmount)
            );
            System.out.println("response :" + updateResponse.getModifiedCount());
            return "updated for " + productId;
        };

        try {
            clientSession.withTransaction(txnBody, txnOptions);
            System.out.println(" ~~ Document committed ~~ ");

            // Release the lock and notify waiting threads
            documentUnlock(productId);
            notifyAll();

            // Process the next request in the queue, if any
            Request nextRequest = requestQueue.poll();
            if (nextRequest != null) {
                return performTransactionWithQueue(nextRequest.getPos(), disbursedAmount, nextRequest.getSessionId());
            }

            return ResponseEntity.ok().body("Success for " + productId + ":" + sessionId);
        } catch (Exception e) {
            clientSession.abortTransaction();
            System.out.println("aborted");
            e.printStackTrace();

            // Release the lock and notify waiting threads
            documentUnlock(productId);
            notifyAll();

            return ResponseEntity.internalServerError().body("Failed for " + productId + ":" + sessionId);
        }
    }

    // Define a class to hold the request details
    private static class Request {
        private Integer pos;
        private String sessionId;

        public Request(Integer pos, String sessionId) {
            this.pos = pos;
            this.sessionId = sessionId;
        }

        public Integer getPos() {
            return pos;
        }

        public String getSessionId() {
            return sessionId;
        }
    }


    private void documentUnlock(String productId) {
        System.out.println("~~ Unlocking Document ~~");
        Bson update = Updates.unset("locked");
        int updated_document = (int) accountCollection.updateOne(Filters.eq("_id", productId), update).getModifiedCount();
        System.out.println("unlocked count :" + updated_document);
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

    private boolean documentLock(String id, Boolean lockStatus) {
        // Lock using writing to mongodb
        System.out.println("~~ Locking Document ~~");
        Bson update = Updates.set("locked", true);
        int updated_document = (int) accountCollection.updateOne(Filters.eq("_id", id), update).getModifiedCount();
        System.out.println("_id -- " + id + " Locked: " + updated_document + " " + lockStatus);
        return true;
    }

    private Boolean isLocked(String id) {
        // For checking lock via find query
        Document document = accountCollection.find(eq("_id", id)).first();
        if (document != null) {
            return document.containsKey("locked") && (Boolean) document.get("locked");
        }
        return false;
    }
}