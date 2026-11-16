package com.exemplo.pagamentos.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "app")
public record AppProperties(String instanceId, Kafka kafka, Core core, Inbox inbox) {

    public AppProperties {
        Duration pior = core.connectTimeout().plus(core.readTimeout());
        if (inbox.lease().compareTo(pior) <= 0) {
            throw new IllegalStateException(
                    "app.inbox.lease (" + inbox.lease() + ") precisa ser maior que connect-timeout + read-timeout (" + pior + ")");
        }
    }

    public record Kafka(String topic, String groupId) {}

    public record Core(String baseUrl, Duration connectTimeout, Duration readTimeout) {}

    public record Inbox(
            Duration lease,
            boolean sweeperEnabled,
            Duration sweeperInterval,
            int sweeperBatchSize,
            int maxNotSentAttempts,
            Duration notSentBackoff,
            Duration retention,
            String purgeCron) {

        public double leaseSeconds() {
            return lease.toMillis() / 1000.0;
        }
    }
}
