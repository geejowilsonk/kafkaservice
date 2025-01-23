package com.DanB;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Properties;

/**
 * The FraudDetectionService class is responsible for detecting suspicious transactions in real-time using Kafka Streams.
 * It monitors the  stream of messages from the 'transaction' topic.
 * This service is responsible to flag the transactions into suspicious category if it is above a certain threshold.
 * Creates a Kafka topic for storing suspicious transactions.
 */
public class FraudDetectionService {

    public static void main(String[] args) {
        // This will create the suspicious_transactions topic and those suspicious transactions are written here.
        createKafkaTopic("suspicious_transactions", 3, (short) 1);

        // Start the Kafka Streams application
        startFraudDetectionStream();
    }

    private static void createKafkaTopic(String topicName, int partitions, short replicationFactor) {
        String bootstrapServers = "localhost:9092";

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(topic));
            System.out.println("Topic '" + topicName + "' created successfully.");
        } catch (Exception e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }

    }

    private static void startFraudDetectionStream() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> transactionsStream = builder.stream("transaction");

        // Fraud detection logic: Flag transactions over 9000 as suspicious
        KStream<String, String> suspiciousTransactions = transactionsStream.filter((key, value) -> {
            JSONObject transaction = new JSONObject(value);
            double amount = transaction.getDouble("amount");
            return amount > 9000;
        });

        //Responsible to write the  suspicious transactions to a new topic
        suspiciousTransactions.to("suspicious_transactions");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println("Fraud Detection Service is running...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Fraud Detection Service...");
            streams.close();
        }));
    }
}
