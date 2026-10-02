package io.github.pratham7711.llmgw.common;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Kafka topic names and Redis key layout. Every per-tenant Redis key carries the {tenant} hash tag,
 * so all of a tenant's keys sit in one Redis Cluster slot and one Lua script can touch them together.
 */
public final class Topics {
  public static final String USAGE_EVENTS = "usage-events";
  public static final String USAGE_EVENTS_DLT = "usage-events.DLT";

  private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZoneOffset.UTC);

  /** Billing month (UTC) of an instant, e.g. "2026-10". */
  public static String month(Instant at) {
    return MONTH.format(at);
  }

  /**
   * A tenant's tokens used this month. The gateway adds each request's billed tokens when it
   * settles, and metering raises it to the Postgres total (set-if-greater), so it is never behind
   * Postgres and never counts a request twice.
   */
  public static String quotaKey(String tenantId, String month) {
    return "quota:used:{" + tenantId + "}:" + month;
  }

  /** Tokens reserved by requests that are admitted but not yet settled. */
  public static String heldKey(String tenantId, String month) {
    return "quota:held:{" + tenantId + "}:" + month;
  }

  public static String bucketKey(String tenantId) {
    return "rl:{" + tenantId + "}";
  }

  /** Hash of lease id to lease record: the request's reservation, then its settled usage event. */
  public static String leaseKey(String tenantId) {
    return "lease:{" + tenantId + "}";
  }

  /** Sorted set of lease ids scored by expiry time (epoch ms). */
  public static String leaseExpiryKey(String tenantId) {
    return "lease:exp:{" + tenantId + "}";
  }

  /** List of expired lease records the reaper has claimed and not yet seen acknowledged by Kafka. */
  public static String reapedKey(String tenantId) {
    return "lease:reaped:{" + tenantId + "}";
  }

  private Topics() {}
}
