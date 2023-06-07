package com.example.demo.api;

import com.example.demo.api.local.Reservation;
import com.example.demo.api.snapshot.RetryMechanism;
import com.example.demo.api.snapshot.WaitMechanism;
import com.example.demo.api.snapshot.QueueWaitMechanism;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;


@RestController
public class DataService {

    private final WaitMechanism waitMechanism;
    private final RetryMechanism retryMechanism;
    private final Reservation localRead;
    private final QueueWaitMechanism queueWait;
    public final String mongodbUri;

    @Autowired
    public DataService(WaitMechanism waitMechanism, RetryMechanism retryMechanism, Reservation reservation, QueueWaitMechanism queueWait, @Value("${uri}") String mongodbUri) {
        this.waitMechanism = waitMechanism;
        this.retryMechanism = retryMechanism;
        this.localRead = reservation;
        this.queueWait = queueWait;
        this.mongodbUri = mongodbUri;
    }

    // For creating a local readConcern transaction
    @PostMapping("/transaction_local/{train_number}/{tier}")
    public ResponseEntity<String> performTransactionWithLocal(@PathVariable String train_number, @PathVariable String tier) {
        return localRead.checkTrainSeats(train_number, tier);
    }

    // For snapshot read concern transaction
    // Option 1: Handling Write Conflicts using transaction wait
    @PostMapping("/transaction_wait/{pos}")
    public ResponseEntity<String> performTransactionWithLock(@PathVariable Integer pos, HttpServletRequest request) throws InterruptedException {
        String sessionId = request.getSession().getId();
        return waitMechanism.performTransactionWithLock(pos, sessionId);
    }

    // Option 2: Handling Write Conflicts using transaction retry
    @PostMapping("/transaction_retry/{pos}")
    public ResponseEntity<Object> performTransactionWithRetry(@PathVariable Integer pos, HttpServletRequest request) {
        String sessionId = request.getSession().getId();
        return retryMechanism.performRetryTransaction(pos, sessionId);
    }

    // Option 3: Handling Write Conflicts using transaction queue
    @PostMapping("/transaction_queue/{pos}/{amount}")
    public ResponseEntity<String> performTransactionWithQueue(@PathVariable Integer pos, @PathVariable Integer amount, HttpServletRequest request) throws InterruptedException {
        String sessionId = request.getSession().getId();
        return queueWait.performTransactionWithQueue(pos, amount, sessionId);
    }
}