package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaAdminClient {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    /**
     * Creates Kafka topics based on the configuration provided.
     * It retries the creation of topics until successful or until the maximum number of retries is reached.
     *
     * @throws KafkaClientException if the maximum number of retries is exceeded or if an error occurs during topic creation.
     */
    public void createTopic() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            LOG.error("Error occurred while creating kafka topic(s): {}", t.getMessage());
            throw new KafkaClientException("Reached max number of retry for creating kafka topic(s)!", t);
        }

        checkTopicsCreated();
    }

    /**
     * Checks if the topics specified in the Kafka configuration have been created.
     * It retries until all topics are confirmed to be created or the maximum number of retries is reached.
     */
    public void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        /*
         * Wait until topics created or max retry reached, increasing wait time exponentially
         * */
        for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while (!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    /**
     * Retrieves the status of the Schema Registry by making a GET request to the configured URL.
     * If the request fails or times out, it returns HttpStatus.SERVICE_UNAVAILABLE.
     *
     * @return HttpStatus representing the status of the Schema Registry.
     */
    private HttpStatus getSchemaRegistryStatus() {
        try {
            return (HttpStatus) webClient
                    .get()
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchangeToMono(response -> Mono.just(response.statusCode()))
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    /**
     * Checks if the specified topic has been created by comparing the list of topics
     * in the Kafka cluster with the expected topic name.
     *
     * @param retryCount The current retry count.
     * @param maxRetry   The maximum number of retries allowed.
     * @throws KafkaClientException if the maximum number of retries is exceeded.
     */
    private void checkMaxRetry(int retryCount, Integer maxRetry) {
        if (retryCount > maxRetry) {
            throw new KafkaClientException("Reached max number of retry for creating kafka topic(s)!");
        }
    }

    /**
     * Sleeps for the specified amount of time in milliseconds.
     *
     * @param sleepTimeMs The time to sleep in milliseconds.
     * @throws KafkaClientException if the thread is interrupted while sleeping.
     */
    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            LOG.error("Thread interrupted while sleeping: {}", e.getMessage());
            throw new KafkaClientException("Thread was interrupted while waiting for topic creation", e);
        }
    }

    /**
     * Checks if the specified topic has been created by looking for it in the list of topics.
     *
     * @param topics The collection of topics currently available in the Kafka cluster.
     * @param topic  The name of the topic to check for existence.
     * @return true if the topic exists, false otherwise.
     */
    private boolean isTopicCreated(Collection<TopicListing> topics, String topic) {
        if (topics == null) return false;

        return topics.stream().anyMatch(t -> t.name().equals(topic));
    }

    /**
     * Creates Kafka topics based on the configuration provided.
     * This method is retried until successful or until the maximum number of retries is reached.
     *
     * @param retryContext The context for the current retry operation.
     * @return CreateTopicsResult containing the result of the topic creation operation.
     */
    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOG.info("Creating {} topic(s), attempt: {}", topicNames.size(), retryContext.getRetryCount());
        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(topic.trim(), kafkaConfigData.getNumOfPartitions(), kafkaConfigData.getReplicationFactor())).toList();

        return adminClient.createTopics(kafkaTopics);
    }

    /**
     * Retrieves the list of topics currently available in the Kafka cluster.
     * This method is retried until successful or until the maximum number of retries is reached.
     *
     * @return A collection of TopicListing objects representing the topics in the Kafka cluster.
     * @throws KafkaClientException if an error occurs while retrieving the topics or if the maximum number of retries is exceeded.
     */
    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;

        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            LOG.error("Error occurred while getting kafka topic(s): {}", t.getMessage());
            throw new KafkaClientException("Reached max number of retry for getting kafka topic(s)!", t);
        }

        return topics;
    }

    /**
     * Retrieves the list of topics currently available in the Kafka cluster.
     * This method is executed within a retry context to handle potential transient errors.
     *
     * @param retryContext The context for the current retry operation.
     * @return A collection of TopicListing objects representing the topics in the Kafka cluster.
     * @throws ExecutionException   if an error occurs while retrieving the topics.
     * @throws InterruptedException if the thread is interrupted while waiting for the result.
     */
    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        LOG.info("Reading kafka topic{} , attempt: {}", kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        LOG.info("Found {} topic(s)", topics.size());

        return topics;
    }

}
