package com.DanB;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.json.JSONObject;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

/**
 * This class serve as ServiceA.
 * This Microservice application is responsible for producing 1 message per second.
 * Here I thought of generating transactions with random mock account number with the amount of transaction.
 * This eventually will create a message in json format and will be ingested to the kafka topic.
 * Kafka packages integrated with Spring is used here to achieve this functionality.
 */
@SpringBootApplication
public class TransactionGeneratorService implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionGeneratorService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(TransactionGeneratorService.class, args);
    }

    @Override
    public void run(String... args) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Random random = new Random();
        scheduler.scheduleAtFixedRate(() -> {
            // Create a JSON object with "transactionId" and other "message" fields
            JSONObject transaction = new JSONObject();

            //Creates account number from 1-100. This will helps in FraudDetection class in efficient join using KTable and KStream
            List<String> accountNumbers = IntStream.range(100000, 100100)
                    .mapToObj(num -> "ACC" + num)
                    .collect(Collectors.toList());

            transaction.put("transactionId", UUID.randomUUID().toString());
            transaction.put("accountNumber", accountNumbers.get(random.nextInt(accountNumbers.size())));
            transaction.put("amount", random.nextDouble() * 10000);
            transaction.put("transactionType", random.nextBoolean() ? "DEPOSIT" : "WITHDRAWAL");
            transaction.put("unixTime", System.currentTimeMillis());
            String dateReadable = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault())
                    .format(Instant.ofEpochMilli(System.currentTimeMillis()));
            transaction.put("dateTime", dateReadable);

            // Convert JSON object to string and send it to Kafka
            String transactionMessage = transaction.toString();
            kafkaTemplate.send("transaction", transaction.getString("accountNumber"), transactionMessage);

            // Log the message for debugging
            System.out.println("Generated transaction: " + transactionMessage);
        }, 0, 1, TimeUnit.SECONDS);
    }
}
