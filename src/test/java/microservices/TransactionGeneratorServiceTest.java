package microservices;

import com.DanB.TransactionGeneratorService;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * This class is the unit test class for TransactionGeneratorService.
 * This class is responsible for validating  TransactionGeneratorService,ie, ServiceA, ie the class that generates 1 message/sec/
 * This ensures that transactions are generated, structured correctly, and published to a Kafka topic as expected.
 *
 */
class TransactionGeneratorServiceTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private TransactionGeneratorService transactionGeneratorService;

    @BeforeEach
    void setUp() {
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        transactionGeneratorService = new TransactionGeneratorService(kafkaTemplate);
    }

    @Test
    void testGeneratedTransaction() {
        Random random = new Random();

        JSONObject transaction = new JSONObject();
        transaction.put("transactionId", UUID.randomUUID().toString());
        transaction.put("accountNumber", "ACC" + (100000 + random.nextInt(900000)));
        transaction.put("amount", random.nextDouble() * 10000);
        transaction.put("transactionType", random.nextBoolean() ? "DEPOSIT" : "WITHDRAWAL");
        transaction.put("unixTime", System.currentTimeMillis());
        String dateReadable = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault())
                .format(Instant.ofEpochMilli(System.currentTimeMillis()));
        transaction.put("dateTime", dateReadable);

        String transactionMessage = transaction.toString();
        kafkaTemplate.send("transaction", transactionMessage);

        // This capture the message sent to Kafka
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("transaction"), captor.capture());

        JSONObject capturedTransaction = new JSONObject(captor.getValue());

        // Below code is to validate the fields
        assertEquals(transaction.getString("transactionId"), capturedTransaction.getString("transactionId"));
        assertEquals(transaction.getString("accountNumber"), capturedTransaction.getString("accountNumber"));
        assertEquals(transaction.getDouble("amount"), capturedTransaction.getDouble("amount"), 0.01);
        assertEquals(transaction.getString("transactionType"), capturedTransaction.getString("transactionType"));
        assertEquals(transaction.getString("dateTime"), capturedTransaction.getString("dateTime"));
    }
}