package io.github.pratham7711.llmgw.metering;

import io.github.pratham7711.llmgw.common.Topics;
import java.time.Clock;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class MeteringConfig {

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  NewTopic usageEvents(@Value("${metering.usage-topic-partitions:6}") int partitions,
      @Value("${metering.usage-topic-replicas:1}") short replicas) {
    return TopicBuilder.name(Topics.USAGE_EVENTS).partitions(partitions).replicas(replicas).build();
  }

  @Bean
  NewTopic usageEventsDlt(@Value("${metering.usage-topic-replicas:1}") short replicas) {
    return TopicBuilder.name(Topics.USAGE_EVENTS_DLT).partitions(1).replicas(replicas).build();
  }

  /** Retry a failed batch indefinitely (0.5 s doubling to 10 s): skipping it would lose billable events. */
  @Bean
  DefaultErrorHandler errorHandler() {
    ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
    backOff.setMaxInterval(10_000);
    return new DefaultErrorHandler(backOff);
  }
}
