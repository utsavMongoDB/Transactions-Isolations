package com.example.demo.api.snapshot;

import com.mongodb.*;
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

import static com.example.demo.api.Helper.create_trx_options;
import static com.mongodb.client.model.Filters.eq;

@Service
public class WaitMechanism {
    public final String mongodbUri;
    static MongoClient mongoClient;
    static MongoDatabase db;
    static MongoCollection<Document> accountCollection;

    @Autowired
    public WaitMechanism(@Value("${uri}") String mongodbUri) {
        this.mongodbUri = mongodbUri;
        ConnectionString connectionString = new ConnectionString(this.mongodbUri);
        mongoClient = MongoClients.create(connectionString);
        db = mongoClient.getDatabase("AbcBank");
        accountCollection = db.getCollection("account");
    }

    public synchronized ResponseEntity<String> performTransactionWithLock(Integer pos, String sessionId) throws InterruptedException {
        String productId = getId(pos);
        Integer disbursedAmount = 50;

        while (isLocked(productId)) {
            // If the document is locked, wait until it's unlocked
            System.out.println("~~ Doc locked ~~"+ productId);
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
            System.out.println("response :"+ updateResponse.getModifiedCount());
            return "updated for "+productId;
        };

        try{
            clientSession.withTransaction(txnBody, txnOptions);
            System.out.println(" ~~ Document committed ~~ ");
            return ResponseEntity.ok().body("Success for " + productId + ":" + sessionId);
        } catch (Exception e) {
            clientSession.abortTransaction();
            System.out.println("aborted");
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed for " + productId + ":" + sessionId);
        }
         finally {
            // Release the lock and notify waiting threads
            documentUnlock(productId);
            notifyAll();
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
