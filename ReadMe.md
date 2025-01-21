How to run 

docker-compose up -d 

mvn spring-boot:run -Dspring-boot.run.main-class=com.DanB.TransactionGeneratorService

kafka
winpty docker exec -it kafka bash

kafka-topics --bootstrap-server localhost:9092 --list

kafka-console-consumer --bootstrap-server localhost:9092 --topic messages --from-beginning


Kibana = http://localhost:5601/