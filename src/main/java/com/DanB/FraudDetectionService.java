package com.DanB;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FraudDetectionService {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final List<String> ACCOUNT_NUMBERS = IntStream.range(100000, 100100)
            .mapToObj(num -> "ACC" + num)
            .collect(Collectors.toList());

    public static void main(String[] args) {
        /**
         * Create necessary Kafka topics. There are two topics being created here.
         *  suspicious_transactions - This topic is to add those transaction details which are possibly be suspicious.
         *  account_info - This topic contains the customer details with their risk score. This risk score is between 0.1 to 1, where 1 being highest.
         */
        createKafkaTopic("suspicious_transactions", 3, (short) 1);
        createKafkaTopic("account_info", 1, (short) 1);

        // Produce sample account data
        produceAccountInfo();

        // Start the Kafka Streams fraud detection service
        startFraudDetectionStream();
    }

    /**
     * The Generic function to create kafka topic.
     */
    private static void createKafkaTopic(String topicName, int partitions, short replicationFactor) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(config)) {
            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(topic));
            System.out.println("Topic '" + topicName + "' created successfully.");
        } catch (Exception e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }
    }

    /**
     * This function produces sample records into the 'account_info' topic.
     */
    private static void produceAccountInfo() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ACCOUNT_NUMBERS.forEach(accountId -> {
                String accountData = String.format(
                        "{ \"accountNumber\": \"%s\", \"riskScore\": %.2f, \"transactionLimit\": %.2f }",
                        accountId, 0.5 + Math.random() * 0.5, 5000 + Math.random() * 5000);

                // Set `accountNumber` as the key
                ProducerRecord<String, String> record = new ProducerRecord<>("account_info", accountId, accountData);

                producer.send(record);
            });

            System.out.println("Account information sent for 100 accounts.");
        }

    }

    /**
     * Starts the Kafka Streams application for fraud detection.
     */
    private static void startFraudDetectionStream() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

// Read transactions as KStream
        KStream<String, String> transactionsStream = builder.stream("transaction");

// Read account_info as a KTable.
        KTable<String, String> accountInfoTable = builder.table("account_info");

// Join transactions with account info
        KStream<String, String> enrichedTransactions = transactionsStream.join(accountInfoTable, (transaction, accountInfo) -> {
            JSONObject txnJson = new JSONObject(transaction);
            JSONObject accJson = new JSONObject(accountInfo);

            double amount = txnJson.getDouble("amount");
            double riskScore = accJson.getDouble("riskScore");
            double transactionLimit = 9000d;

            System.out.println("amount is " + amount);
            System.out.println("riskScore is " + riskScore);

            // Fraud detection business rule.
            boolean isFraud = riskScore >= 0.7 && amount > transactionLimit;
            txnJson.put("isFraud", isFraud);
            txnJson.put("riskScore", riskScore);

            return txnJson.toString();
        });

// Filter fraudulent transactions
        KStream<String, String> suspiciousTransactions = enrichedTransactions
                .filter((key, value) -> new JSONObject(value).getBoolean("isFraud"));

// Write fraud alerts to suspicious_transactions topic
        suspiciousTransactions.to("suspicious_transactions");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println(" Fraud Detection Service is running");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Fraud Detection Service");
            streams.close();
        }));

    }

}
