package com.exemplo.pagamentos.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic pagamentosTopic(AppProperties props) {
        return TopicBuilder.name(props.kafka().topic()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic pagamentosDltTopic(AppProperties props) {
        return TopicBuilder.name(props.kafka().topic() + ".DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template);
        var backOff = new ExponentialBackOff(1_000, 2.0);
        backOff.setMaxElapsedTime(30_000);
        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(DeserializationException.class, IllegalArgumentException.class);
        return handler;
    }
}
