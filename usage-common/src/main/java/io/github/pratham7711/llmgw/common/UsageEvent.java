package io.github.pratham7711.llmgw.common;

import java.time.Instant;
import java.util.UUID;

/**
 * One completed (or failed) upstream call, as billed. The gateway emits exactly one event per
 * forwarded request; the metering service deduplicates on {@code eventId}, so redelivery after a
 * consumer crash or a producer retry can never bill a request twice.
 *
 * {@code eventId} is also the id of the request's lease in Redis (see docs/DESIGN.md, "Leases"),
 * so an event the gateway publishes and the one the lease reaper publishes after a gateway crash
 * collapse into one billing row.
 *
 * {@code usageSource} says where the token counts came from: the provider's own usage report, the
 * gateway's tokenizer (the provider never reported usage, e.g. a stream cut short), nothing at all
 * (an error response), or an expired lease billed at its reservation. {@code estTokensIn} and
 * {@code estTokensOut} are the gateway's own counts, kept next to the provider's so the two can be
 * compared; {@code estTokensOut} is null when it was not computed.
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
    Instant occurredAt,
    String usageSource,
    Integer estTokensIn,
    Integer estTokensOut) {

  public static final int SCHEMA_VERSION = 2;

  public static final String SOURCE_PROVIDER = "provider";
  public static final String SOURCE_ESTIMATED = "estimated";
  public static final String SOURCE_NONE = "none";
  public static final String SOURCE_LEASE_EXPIRED = "lease_expired";

  /** Schema version 1 shape: provider-reported usage, no local estimate. */
  public UsageEvent(UUID eventId, String tenantId, String model, int tokensIn, int tokensOut, long latencyMs,
      int status, boolean streamed, Instant occurredAt) {
    this(eventId, tenantId, model, tokensIn, tokensOut, latencyMs, status, streamed, occurredAt,
        SOURCE_PROVIDER, null, null);
  }

  public long totalTokens() {
    return (long) tokensIn + tokensOut;
  }

  /** Version 1 events carry no source; they were all provider-reported. */
  public String source() {
    return usageSource == null ? SOURCE_PROVIDER : usageSource;
  }

  public UsageEvent withTokens(int in, int out) {
    return new UsageEvent(eventId, tenantId, model, in, out, latencyMs, status, streamed, occurredAt,
        usageSource, estTokensIn, estTokensOut);
  }

  public UsageEvent withTokensOut(int out) {
    return new UsageEvent(eventId, tenantId, model, tokensIn, out, latencyMs, status, streamed, occurredAt,
        usageSource, estTokensIn, estTokensOut);
  }

  /** Returns null when the event is usable, otherwise the reason it must go to the dead-letter topic. */
  public String validationError() {
    if (eventId == null) return "missing eventId";
    if (tenantId == null || tenantId.isBlank()) return "missing tenantId";
    if (model == null || model.isBlank()) return "missing model";
    if (tokensIn < 0 || tokensOut < 0) return "negative token count";
    if (latencyMs < 0) return "negative latency";
    String source = source();
    if (!source.equals(SOURCE_PROVIDER) && !source.equals(SOURCE_ESTIMATED) && !source.equals(SOURCE_NONE)
        && !source.equals(SOURCE_LEASE_EXPIRED)) {
      return "unknown usageSource " + source;
    }
    // A lease that expired never saw a response, so it has no HTTP status.
    boolean noStatus = status == 0 && source.equals(SOURCE_LEASE_EXPIRED);
    if (!noStatus && (status < 100 || status > 599)) return "invalid status " + status;
    if (occurredAt == null) return "missing occurredAt";
    return null;
  }
}
