package com.example.demo.api;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;

public class Helper {

    public static TransactionOptions create_trx_options(ReadConcern readConcern) {
        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(readConcern)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        return txnOptions;
    }
}
