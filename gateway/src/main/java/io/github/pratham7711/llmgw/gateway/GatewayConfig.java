package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.Topics;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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

  /**
   * Deadlines for open streams. Cancelled deadlines are removed at once rather than when they would
   * have fired, or thousands of short streams would each leave a task queued for the full duration.
   */
  @Bean(destroyMethod = "shutdownNow")
  ScheduledExecutorService streamDeadlines() {
    ScheduledThreadPoolExecutor ex = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().name("stream-deadline").daemon().factory());
    ex.setRemoveOnCancelPolicy(true);
    return ex;
  }

  @Bean
  NewTopic usageEvents(GatewayProperties props, @Value("${gateway.usage-topic-replicas:1}") short replicas) {
    return TopicBuilder.name(Topics.USAGE_EVENTS).partitions(props.usageTopicPartitions()).replicas(replicas).build();
  }
}
