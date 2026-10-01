package io.github.pratham7711.llmgw.common;

import java.time.Instant;
import java.util.UUID;

/**
 * One completed (or failed) upstream call, as billed. The gateway emits exactly one event per
 * forwarded request; the metering service deduplicates on {@code eventId}, so redelivery after a
 * consumer crash or a producer retry can never bill a request twice.
 */
public record UsageEvent(
    UUID eventId,
    String tenantId,
    String model,
    int tokensIn,
    int tokensOut,
    long latencyMs,
    int status,
    boolean streamed,
    Instant occurredAt) {

  public static final int SCHEMA_VERSION = 1;

  public long totalTokens() {
    return (long) tokensIn + tokensOut;
  }

  /** Returns null when the event is usable, otherwise the reason it must go to the dead-letter topic. */
  public String validationError() {
    if (eventId == null) return "missing eventId";
    if (tenantId == null || tenantId.isBlank()) return "missing tenantId";
    if (model == null || model.isBlank()) return "missing model";
    if (tokensIn < 0 || tokensOut < 0) return "negative token count";
    if (latencyMs < 0) return "negative latency";
    if (status < 100 || status > 599) return "invalid status " + status;
    if (occurredAt == null) return "missing occurredAt";
    return null;
  }
}
