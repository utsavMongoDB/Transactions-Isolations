package com.example.demo.api.majority;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class Reservation {

    static MongoClient mongoClient;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    public final String mongodbUri;

    @Autowired
    public Reservation(@Value("${uri}") String mongodbUri) {
        this.mongodbUri = mongodbUri;
        ConnectionString connectionString = new ConnectionString(this.mongodbUri);
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase("trainReservation");
        collection = database.getCollection("trains");
    }

    public ResponseEntity<String> checkTrainSeats(String train_number, String tier) {
        try {
            // Configure transaction options
            TransactionOptions txnOptions = TransactionOptions.builder()
                    .readConcern(ReadConcern.MAJORITY)
                    .writeConcern(WriteConcern.MAJORITY)
                    .build();

            ClientSession clientSession = mongoClient.startSession();
            // Perform the transaction
            clientSession.withTransaction(() -> {
                // Function to check the number of available seats in a train
                int availableSeats = checkSeatsAvailable(collection, train_number, tier);

                // If seats are available, book a seat
                if (availableSeats > 0) {
//                    bookSeat(collection, train_number, tier);
                    return ResponseEntity.ok().body("Seat booked successfully!");
                } else {
                    return ResponseEntity.ok().body("No seats available!");
                }
            }, txnOptions);

            // Close the MongoDB client
            mongoClient.close();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            return ResponseEntity.internalServerError().body("Server Error Occurred");
        }
        return null;
    }

    private static int checkSeatsAvailable(MongoCollection<Document> collection, String trainNumber, String tier) {
        // Filter to find the train by train number
        Bson filter = Filters.eq("trainNumber", trainNumber);

        // Projection to include only the desired tier's seat information
        Bson projection = Projections.elemMatch("seats", Filters.eq("tier", tier));

        // Find the train document with the specified train number and retrieve the desired tier's seat information
        Document trainDocument = collection.find(filter).projection(projection).first();
        System.out.println(trainDocument);

        // If the train document is found and the tier seat information is available, return the number of available seats
        if (trainDocument != null) {
            Integer availableSeats = trainDocument.getInteger("seats.0.availableSeats");
            if (availableSeats != null) {
                return availableSeats;
            }
        }
        // If the train or tier information is not found, return -1 indicating an error
        return -1;
    }

    private static void bookSeat(MongoCollection<Document> collection, String trainNumber, String tier) {
        // Filter to find the train by train number
        Bson filter = Filters.eq("trainNumber", trainNumber);

        // Filter to find the seat by tier
        Bson seatFilter = Filters.and(filter, Filters.eq("seats.tier", tier));

        // Update to decrement the availableSeats count and mark the seat as booked
        Bson update = Updates.combine(
                Updates.inc("seats.$.availableSeats", -1),
                Updates.inc("seats.$.bookedSeats", 1)
        );

        // Perform the update operation
        collection.updateOne(seatFilter, update);
    }
}
