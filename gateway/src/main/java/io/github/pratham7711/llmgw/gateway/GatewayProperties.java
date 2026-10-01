package io.github.pratham7711.llmgw.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(
    String upstreamBaseUrl,
    String upstreamApiKey,
    Duration upstreamConnectTimeout,
    Duration upstreamRequestTimeout,
    int usageTopicPartitions) {

  public GatewayProperties {
    if (upstreamBaseUrl == null || upstreamBaseUrl.isBlank()) upstreamBaseUrl = "http://localhost:8090";
    if (upstreamConnectTimeout == null) upstreamConnectTimeout = Duration.ofSeconds(2);
    if (upstreamRequestTimeout == null) upstreamRequestTimeout = Duration.ofSeconds(120);
    if (usageTopicPartitions <= 0) usageTopicPartitions = 6;
  }
}
