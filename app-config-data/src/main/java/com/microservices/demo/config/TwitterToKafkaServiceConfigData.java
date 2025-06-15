package com.microservices.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {
    private List<String> twitterKeywords;

    private String welcomeMessage;

    private Boolean enableMockTweets;

    private Integer mockMinTweetLength;

    private Integer mockMaxTweetLength;

    private Long mockSleepMs;
}
