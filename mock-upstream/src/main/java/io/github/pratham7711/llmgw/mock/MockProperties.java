package io.github.pratham7711.llmgw.mock;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Latency is log-normal around {@code medianMs}: {@code median * exp(sigma * Z)}, clamped to
 * {@code maxMs}. With the defaults (40 ms, sigma 0.35) p50 is about 40 ms and p99 about 90 ms.
 */
@ConfigurationProperties(prefix = "mock")
public record MockProperties(
    long medianMs,
    double sigma,
    long maxMs,
    double errorRate,
    int streamChunks) {

  public MockProperties {
    if (medianMs <= 0) medianMs = 40;
    if (sigma < 0) sigma = 0.35;
    if (maxMs <= 0) maxMs = 2000;
    if (errorRate < 0 || errorRate > 1) errorRate = 0;
    if (streamChunks <= 0) streamChunks = 8;
  }
}
