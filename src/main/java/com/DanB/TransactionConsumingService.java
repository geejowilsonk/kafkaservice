package com.DanB;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;


@SpringBootApplication
public class TransactionConsumingService {

    private final LoggingService loggingService;

    public TransactionConsumingService(LoggingService loggingService) {
        this.loggingService = loggingService;
    }

    public static void main(String[] args) {

        SpringApplication.run(TransactionConsumingService.class, args);
    }

    @KafkaListener(topics = "transaction", groupId = "transaction_monitoring_group")
    public void consume(String transactionMessage) {
        System.out.println("Received transaction is jpp::: " + transactionMessage);
        loggingService.ingestLogToElastic(transactionMessage);
    }
}

@Component
class LoggingService {

    private final RestTemplate restTemplate;

    public LoggingService() {
        this.restTemplate = new RestTemplate();
    }

    public void ingestLogToElastic(String logMessage) {
        String elasticUrl = "http://localhost:9200/transactions/_doc/";

        try {
            // Create HTTP headers
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

            // Wrap the log message into HttpEntity
            org.springframework.http.HttpEntity<String> request = new org.springframework.http.HttpEntity<>(logMessage, headers);

            // Send POST request
            restTemplate.postForEntity(elasticUrl, request, String.class);
            System.out.println("TransactionMessages ingested successfully to Elasticsearch.");
        } catch (Exception e) {
            System.err.println("Failed to ingest TransactionMessages to Elasticsearch: " + e.getMessage());
        }
    }
}

