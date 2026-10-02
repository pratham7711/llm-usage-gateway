package io.github.pratham7711.llmgw.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(
    String upstreamBaseUrl,
    String upstreamApiKey,
    Duration upstreamConnectTimeout,
    Duration upstreamRequestTimeout,
    int usageTopicPartitions,
    /** A stream still open after this long is cut and billed for what it carried. */
    Duration maxStreamDuration,
    /** How long a request's lease lives before the reaper bills it as abandoned. */
    Duration leaseTtl,
    /** Output tokens reserved, and sent upstream as max_tokens, when a request sets no limit. */
    int defaultMaxOutputTokens,
    /** Headroom on the local prompt count when reserving, for tokenizers that differ from the provider's. */
    double promptEstimateMargin,
    /** Share of responses whose output the gateway also counts itself, to track drift from the provider. */
    double estimateSampleRate,
    /** Admission refuses a tenant with this many unsettled or unacknowledged requests. */
    int maxOpenLeasesPerTenant) {

  public GatewayProperties {
    if (upstreamBaseUrl == null || upstreamBaseUrl.isBlank()) upstreamBaseUrl = "http://localhost:8090";
    if (upstreamConnectTimeout == null) upstreamConnectTimeout = Duration.ofSeconds(2);
    if (upstreamRequestTimeout == null) upstreamRequestTimeout = Duration.ofSeconds(120);
    if (usageTopicPartitions <= 0) usageTopicPartitions = 6;
    if (maxStreamDuration == null) maxStreamDuration = Duration.ofMinutes(10);
    if (leaseTtl == null) leaseTtl = Duration.ofMinutes(11);
    if (defaultMaxOutputTokens <= 0) defaultMaxOutputTokens = 4096;
    if (promptEstimateMargin < 0) promptEstimateMargin = 0;
    if (estimateSampleRate < 0 || estimateSampleRate > 1) estimateSampleRate = 0.01;
    if (maxOpenLeasesPerTenant <= 0) maxOpenLeasesPerTenant = 100_000;

    // A lease that expired while its request was still running would be billed at its reservation
    // by the reaper, and the real event would then be dropped as a duplicate of it.
    Duration longest = maxStreamDuration.compareTo(upstreamRequestTimeout) > 0 ? maxStreamDuration : upstreamRequestTimeout;
    if (leaseTtl.compareTo(longest.plusSeconds(30)) < 0) {
      throw new IllegalArgumentException("gateway.lease-ttl (" + leaseTtl + ") must exceed the longest request ("
          + longest + ") by at least 30s");
    }
  }
}
