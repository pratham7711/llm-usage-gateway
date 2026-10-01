package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.Topics;
import java.time.Clock;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class GatewayConfig {

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  NewTopic usageEvents(GatewayProperties props, @Value("${gateway.usage-topic-replicas:1}") short replicas) {
    return TopicBuilder.name(Topics.USAGE_EVENTS).partitions(props.usageTopicPartitions()).replicas(replicas).build();
  }
}
