#!/bin/bash
# This script is responsible for finding the consumer performance
# A test message - 100 is coded in the scrpit, which can be changed depending on the size of the topic
# This uses the kafka-consumer-perf-test in kafka path
# Lightweight script for quick performance check.
# Possiblities - Can be extended to producers and all topics.

KAFKA_CONTAINER="kafka"

# Kafka configuration
BROKER="localhost:9092"
TOPIC="transaction"
GROUP="test-group"
MESSAGES=100

echo "Running Kafka consumer performance test ."
winpty docker exec -it $KAFKA_CONTAINER bash -c "kafka-consumer-perf-test \
  --bootstrap-server $BROKER \
  --topic $TOPIC \
  --group $GROUP \
  --messages $MESSAGES"

if [ $? -eq 0 ]; then
  echo "Kafka consumer performance test completed successfully."
else
  echo "Kafka consumer performance test failed."
  exit 1
fi
