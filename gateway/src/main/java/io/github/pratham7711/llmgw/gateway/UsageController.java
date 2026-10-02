package io.github.pratham7711.llmgw.gateway;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pratham7711.llmgw.common.Topics;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * A tenant's own usage for a month, two ways: "live" is what admission sees right now in Redis
 * (tokens used plus tokens reserved by requests in flight), "billed" is what metering has committed
 * to Postgres, with cost. Billed lags live by the consumer lag; neither ever exceeds the quota.
 */
@RestController
public class UsageController {

  private static final Pattern MONTH = Pattern.compile("\\d{4}-\\d{2}");

  private final TenantDirectory tenants;
  private final StringRedisTemplate redis;
  private final JdbcClient jdbc;
  private final Clock clock;
  /** Billing rows change at most once per consumer batch; a short cache keeps polling off Postgres. */
  private final Cache<String, Map<String, Object>> billed =
      Caffeine.newBuilder().maximumSize(10_000).expireAfterWrite(Duration.ofSeconds(2)).build();

  public UsageController(TenantDirectory tenants, StringRedisTemplate redis, JdbcClient jdbc, Clock clock) {
    this.tenants = tenants;
    this.redis = redis;
    this.jdbc = jdbc;
    this.clock = clock;
  }

  @GetMapping("/v1/usage")
  public ResponseEntity<?> usage(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth,
      @RequestParam(required = false) String month) {
    String key = auth != null && auth.regionMatches(true, 0, "Bearer ", 0, 7) ? auth.substring(7).trim() : null;
    Optional<Tenant> resolved = tenants.resolve(key);
    if (resolved.isEmpty()) {
      return ResponseEntity.status(401).body(Map.of("error", Map.of("type", "invalid_api_key", "message", "Missing or invalid API key")));
    }
    Tenant tenant = resolved.get();
    String m = month == null ? Topics.month(clock.instant()) : month;
    if (!MONTH.matcher(m).matches()) {
      return ResponseEntity.status(400).body(Map.of("error", Map.of("type", "invalid_request_error", "message", "month must be YYYY-MM")));
    }

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("tenant", tenant.id());
    body.put("month", m);
    body.put("quota_tokens", tenant.monthlyTokenQuota());
    body.put("live", live(tenant, m));
    body.putAll(billed.get(tenant.id() + "|" + m, k -> billed(tenant.id(), m)));
    return ResponseEntity.ok(body);
  }

  private Map<String, Object> live(Tenant tenant, String month) {
    try {
      List<String> v = redis.opsForValue().multiGet(List.of(Topics.quotaKey(tenant.id(), month), Topics.heldKey(tenant.id(), month)));
      long used = v == null || v.get(0) == null ? 0 : Long.parseLong(v.get(0));
      long held = v == null || v.get(1) == null ? 0 : Long.parseLong(v.get(1));
      Map<String, Object> live = new LinkedHashMap<>();
      live.put("used_tokens", used);
      live.put("held_tokens", held);
      live.put("remaining_tokens", Math.max(0, tenant.monthlyTokenQuota() - used - held));
      return live;
    } catch (DataAccessException e) {
      return null;
    }
  }

  private Map<String, Object> billed(String tenantId, String month) {
    YearMonth ym = YearMonth.parse(month);
    OffsetDateTime from = ym.atDay(1).atStartOfDay().atOffset(ZoneOffset.UTC);
    OffsetDateTime to = ym.plusMonths(1).atDay(1).atStartOfDay().atOffset(ZoneOffset.UTC);

    Map<String, Object> total = jdbc.sql("""
            select tokens, requests, cost_nano_usd from tenant_month_usage where tenant_id = ? and month = ?
            """).param(tenantId).param(month)
        .query((rs, n) -> {
          Map<String, Object> t = new LinkedHashMap<>();
          t.put("tokens", rs.getLong(1));
          t.put("requests", rs.getLong(2));
          t.put("cost_usd", usd(rs.getLong(3)));
          return t;
        }).optional().orElseGet(() -> {
          Map<String, Object> t = new LinkedHashMap<>();
          t.put("tokens", 0L);
          t.put("requests", 0L);
          t.put("cost_usd", usd(0));
          return t;
        });

    List<Map<String, Object>> byModel = jdbc.sql("""
            select model, sum(requests), sum(errors), sum(tokens_in), sum(tokens_out), sum(cost_nano_usd)
            from usage_rollup_minute
            where tenant_id = ? and minute >= ? and minute < ?
            group by model order by model
            """).param(tenantId).param(from).param(to)
        .query((rs, n) -> {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("model", rs.getString(1));
          row.put("requests", rs.getLong(2));
          row.put("errors", rs.getLong(3));
          row.put("tokens_in", rs.getLong(4));
          row.put("tokens_out", rs.getLong(5));
          row.put("cost_usd", usd(rs.getLong(6)));
          return row;
        }).list();

    Map<String, Object> out = new LinkedHashMap<>();
    out.put("billed", total);
    out.put("by_model", byModel);
    return out;
  }

  private static String usd(long nanoUsd) {
    return BigDecimal.valueOf(nanoUsd, 9).stripTrailingZeros().toPlainString();
  }
}
