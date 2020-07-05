# Kafka-Streamer-Histogram-Data

#### Get Docker kafka running using docker-compose file ####
docker-compose -f kafka.yaml up -d 

#### Copy package json file into the container ####
  * docker ps | grep bitnami/kafka  (fetch container id of kafka)
  * docker cp package.json <container_id>:/package.json

#### Run kafka console producer for the data ####
  * docker exec -it <container_id> /bin/sh
  * kafka-console-producer.sh --broker-list localhost:9092 --topic test < package.json

#### Run Java Application to get histogram ####



