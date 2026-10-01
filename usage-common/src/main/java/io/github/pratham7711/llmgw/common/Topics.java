package io.github.pratham7711.llmgw.common;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class Topics {
  public static final String USAGE_EVENTS = "usage-events";
  public static final String USAGE_EVENTS_DLT = "usage-events.DLT";

  private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZoneOffset.UTC);

  /** Billing month (UTC) of an instant, e.g. "2026-10". */
  public static String month(Instant at) {
    return MONTH.format(at);
  }

  /**
   * Redis key caching a tenant's billed tokens for a month, written by metering and read by the
   * gateway. The {tenant} hash tag keeps it in the same cluster slot as the rate-limit bucket, so
   * both can be checked by one Lua script.
   */
  public static String quotaKey(String tenantId, String month) {
    return "quota:used:{" + tenantId + "}:" + month;
  }

  public static String bucketKey(String tenantId) {
    return "rl:{" + tenantId + "}";
  }

  private Topics() {}
}
