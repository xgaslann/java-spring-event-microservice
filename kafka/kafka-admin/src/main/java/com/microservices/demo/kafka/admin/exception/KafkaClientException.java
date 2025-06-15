package com.microservices.demo.kafka.admin.exception;

/**
 * Custom exception class for Kafka client operations.
 * This exception is thrown when there are issues with Kafka client operations.
 */
public class KafkaClientException extends RuntimeException {
    public KafkaClientException() {}

    public KafkaClientException(String message) {
        super(message);
    }

    public KafkaClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
