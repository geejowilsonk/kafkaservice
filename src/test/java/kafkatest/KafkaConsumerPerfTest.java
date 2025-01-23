package kafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * KafkaConsumerPerfTest is a utility class designed to evaluate the performance of a Kafka consumer by consuming a specified number of messages from a Kafka topic.
 * The test outputs the time taken to consume messages and calculates the throughput in terms of messages per second.
 * This is a basic test class based on the num of messages polled and time taken.
 */
public class KafkaConsumerPerfTest {

    public static void main(String[] args) {
        // Kafka consumer configuration settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // This is to Create Kafka consumer instance
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic that is already present in the clister.
        String topic = "transaction";
        consumer.subscribe(Collections.singletonList(topic));

        long startTime = System.currentTimeMillis(); // Start time
        long messageCount = 0;
        long maxMessages = 100; // Maximum number of messages to consume, this is currently set to 100 for the topic, which generates 1 message/sec.

        System.out.println("Starting consumer performance test.");

        try {
            while (messageCount < maxMessages) {
                // This polls the messages from the topic
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;

                    System.out.printf("Consumed message: key=%s, value=%s, offset=%d%n",
                            record.key(), record.value(), record.offset());
                //stopps the polling
                    if (messageCount >= maxMessages) {
                        break;
                    }
                }
            }
        } finally {
            consumer.close();

            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;
            double messagesPerSecond = (double) messageCount / (elapsedTime / 1000.0);

            System.out.printf("\nConsumed %d messages in %d ms (%.2f messages/second)%n",
                    messageCount, elapsedTime, messagesPerSecond);
        }
    }
}
