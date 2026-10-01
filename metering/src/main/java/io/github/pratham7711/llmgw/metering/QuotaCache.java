package io.github.pratham7711.llmgw.metering;

import io.github.pratham7711.llmgw.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Copies the authoritative monthly totals from Postgres into the Redis keys the gateway reads.
 * Writes only ever raise a value ("set if greater"), so a late or replayed write cannot roll a
 * tenant's usage back. A failed write does not fail the batch: the periodic resync repairs it.
 */
@Component
public class QuotaCache {

  private static final Logger log = LoggerFactory.getLogger(QuotaCache.class);
  private static final long TTL_SECONDS = Duration.ofDays(40).toSeconds();
  private static final DefaultRedisScript<Long> SET_IF_GREATER = new DefaultRedisScript<>("""
      local cur = tonumber(redis.call('GET', KEYS[1]) or '-1')
      if tonumber(ARGV[1]) > cur then
        redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
        return 1
      end
      return 0
      """, Long.class);

  private final StringRedisTemplate redis;
  private final UsageStore store;
  private final Clock clock;
  private final Counter writeFailures;

  public QuotaCache(StringRedisTemplate redis, UsageStore store, Clock clock, MeterRegistry meters) {
    this.redis = redis;
    this.store = store;
    this.clock = clock;
    this.writeFailures = meters.counter("metering.quota.write.failures");
  }

  public void writeBack(List<UsageStore.MonthTotal> totals) {
    for (UsageStore.MonthTotal t : totals) {
      try {
        redis.execute(SET_IF_GREATER, List.of(Topics.quotaKey(t.tenantId(), t.month())),
            String.valueOf(t.tokens()), String.valueOf(TTL_SECONDS));
      } catch (RuntimeException e) {
        writeFailures.increment();
        log.warn("quota write-back failed for {} {}: {}", t.tenantId(), t.month(), e.toString());
      }
    }
  }

  @Scheduled(fixedDelayString = "${metering.quota-resync-interval:30s}", initialDelayString = "10s")
  public void resync() {
    try {
      writeBack(store.monthTotals(Topics.month(clock.instant())));
    } catch (RuntimeException e) {
      log.warn("quota resync failed: {}", e.toString());
    }
  }
}
