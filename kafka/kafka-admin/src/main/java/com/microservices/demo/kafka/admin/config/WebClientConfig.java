package com.microservices.demo.kafka.admin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {
    /**
     * Creates a WebClient bean for making HTTP requests.
     * This WebClient can be used to interact with external services.
     *
     * @return a configured WebClient instance
     */
    @Bean
    WebClient webClient(){
        return WebClient.builder().build();
    }
}
