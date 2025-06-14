### Example of .env file for the project

### .env.example
#### COMPOSE_PATH_SEPARATOR=:
#### COMPOSE_FILE=common.yml:kafka_cluster.yml
#### KAFKA_VERSION=5.0.4
#### ELASTIC_VERSION=7.7.1
#### SERVICE_VERSION=0.0.1-SNAPSHOT ### project version
#### GLOBAL_NETWORK=application
#### GROUP_ID=com.microservices


### docker-compose -f common.yml -f kafka_cluster.yml up

#### Kafka
#### Kafka Model
- Create and hold Java Objects for Kafka in Avro Format
#### Kafka Admin
- Create and verify Kafka topics programmatically
#### Kafka Producer
- Use spring-kafka to write Kafka producer implementation