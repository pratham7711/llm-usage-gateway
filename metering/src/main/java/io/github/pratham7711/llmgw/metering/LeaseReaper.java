package io.github.pratham7711.llmgw.metering;

import io.github.pratham7711.llmgw.common.LeaseRecord;
import io.github.pratham7711.llmgw.common.Topics;
import io.github.pratham7711.llmgw.common.UsageEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Bills the requests a gateway admitted but never finished publishing.
 *
 * Every admitted request holds a lease in Redis until Kafka has acknowledged its usage event. A
 * lease still there after its ttl belongs to a gateway that crashed, lost Redis, or could not reach
 * Kafka. The reaper claims it (reap.lua), publishes its event under the lease id, and forgets it
 * only once Kafka acknowledges. A lease that was settled carries the exact event; one that was
 * never settled is billed at its reservation, the most the request could have cost.
 *
 * Every step is safe to repeat and safe to run on several replicas at once: claiming is atomic, a
 * republished event has the same id as the original and the sink drops the duplicate, and a
 * reaped record is removed by value (LREM), so one replica can never remove another's.
 */
@Component
public class LeaseReaper {

  private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);
  private static final int CLAIM_BATCH = 1000;
  private static final int PUBLISH_BATCH = 1000;
  private static final int MAX_CLAIMS_PER_TENANT = 50;
  private static final long MONTH_KEY_TTL_SECONDS = Duration.ofDays(40).toSeconds();

  @SuppressWarnings("rawtypes")
  private final DefaultRedisScript<List> reap;
  private final StringRedisTemplate redis;
  private final JdbcTemplate jdbc;
  private final KafkaTemplate<String, String> kafka;
  private final JsonMapper json;
  private final Clock clock;
  private final Counter reapedHeld;
  private final Counter reapedSettled;
  private final Counter stale;
  private final Counter published;
  private final Counter publishFailed;
  private final Counter malformed;

  public LeaseReaper(StringRedisTemplate redis, JdbcTemplate jdbc, KafkaTemplate<String, String> kafka,
      JsonMapper json, Clock clock, MeterRegistry meters) {
    this.redis = redis;
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.json = json;
    this.clock = clock;
    this.reap = new DefaultRedisScript<>();
    this.reap.setLocation(new ClassPathResource("reap.lua"));
    this.reap.setResultType(List.class);
    this.reapedHeld = meters.counter("metering.leases", "result", "reaped_unsettled");
    this.reapedSettled = meters.counter("metering.leases", "result", "reaped_settled");
    this.stale = meters.counter("metering.leases", "result", "stale");
    this.published = meters.counter("metering.leases", "result", "published");
    this.publishFailed = meters.counter("metering.leases", "result", "publish_failed");
    this.malformed = meters.counter("metering.leases", "result", "malformed");
  }

  @Scheduled(fixedDelayString = "${metering.lease-reap-interval:10s}", initialDelayString = "5s")
  public void run() {
    try {
      reapAll();
    } catch (RuntimeException e) {
      log.warn("lease reap failed: {}", e.toString());
    }
  }

  /** One pass over every tenant; returns the number of events published. */
  public int reapAll() {
    List<String> tenants = jdbc.queryForList("select id from tenant order by id", String.class);
    if (tenants.isEmpty()) return 0;
    long now = clock.millis();
    // One pipelined round trip finds the few tenants with anything to do.
    // A pipelined callback gets the raw connection, not the String wrapper, so keys go as bytes.
    List<Object> sizes = redis.executePipelined((RedisCallback<Object>) c -> {
      for (String t : tenants) {
        c.zSetCommands().zCount(bytes(Topics.leaseExpiryKey(t)), Double.NEGATIVE_INFINITY, now);
        c.listCommands().lLen(bytes(Topics.reapedKey(t)));
      }
      return null;
    });
    int sent = 0;
    for (int i = 0; i < tenants.size(); i++) {
      long expired = ((Number) sizes.get(2 * i)).longValue();
      long pending = ((Number) sizes.get(2 * i + 1)).longValue();
      if (expired > 0) claim(tenants.get(i));
      if (expired > 0 || pending > 0) sent += publish(tenants.get(i));
    }
    return sent;
  }

  private void claim(String tenant) {
    Deque<String> months = new ArrayDeque<>(List.of(Topics.month(clock.instant())));
    Set<String> queued = new HashSet<>(months);
    long total = 0, unsettled = 0;
    for (int calls = 0; calls < MAX_CLAIMS_PER_TENANT && !months.isEmpty(); calls++) {
      String month = months.poll();
      List<?> r = redis.execute(reap,
          List.of(Topics.leaseKey(tenant), Topics.leaseExpiryKey(tenant), Topics.reapedKey(tenant),
              Topics.heldKey(tenant, month), Topics.quotaKey(tenant, month)),
          month, String.valueOf(CLAIM_BATCH), String.valueOf(MONTH_KEY_TTL_SECONDS));
      long claimed = ((Number) r.get(0)).longValue();
      long held = ((Number) r.get(1)).longValue();
      long gone = ((Number) r.get(2)).longValue();
      total += claimed;
      unsettled += held;
      reapedHeld.increment(held);
      reapedSettled.increment(claimed - held);
      stale.increment(gone);
      for (Object other : r.subList(3, r.size())) {
        String m = String.valueOf(other);
        if (queued.add(m)) months.add(m);
      }
      // A call that claimed anything may have left more behind its batch limit.
      if (claimed + gone > 0) months.addFirst(month);
      else queued.remove(month);
    }
    if (total > 0) log.info("tenant {}: claimed {} expired leases, {} never settled", tenant, total, unsettled);
  }

  private int publish(String tenant) {
    String key = Topics.reapedKey(tenant);
    List<String> raw = redis.opsForList().range(key, 0, PUBLISH_BATCH - 1);
    if (raw == null || raw.isEmpty()) return 0;

    List<String> sentValues = new ArrayList<>(raw.size());
    List<CompletableFuture<?>> sends = new ArrayList<>(raw.size());
    List<String> drop = new ArrayList<>();
    for (String v : raw) {
      try {
        UsageEvent e = toEvent(v);
        sends.add(kafka.send(Topics.USAGE_EVENTS, e.tenantId(), json.writeValueAsString(e)));
        sentValues.add(v);
      } catch (IllegalArgumentException | JacksonException e) {
        malformed.increment();
        log.error("dropping unreadable lease record for tenant {}: {}", tenant, e.toString());
        drop.add(v);
      }
    }

    List<String> done = new ArrayList<>(drop);
    for (int i = 0; i < sends.size(); i++) {
      try {
        sends.get(i).get(30, TimeUnit.SECONDS);
        done.add(sentValues.get(i));
      } catch (Exception e) {
        // Left in the list; the next pass sends it again under the same id.
        publishFailed.increment();
        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
      }
    }
    if (!done.isEmpty()) {
      redis.executePipelined((RedisCallback<Object>) c -> {
        for (String v : done) c.listCommands().lRem(bytes(key), 1, bytes(v));
        return null;
      });
    }
    int ok = done.size() - drop.size();
    published.increment(ok);
    return ok;
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** The event a reaped record bills: the settled event as is, or the reservation of an unsettled one. */
  UsageEvent toEvent(String raw) {
    LeaseRecord lease = LeaseRecord.parse(raw);
    UsageEvent e = json.readValue(lease.eventJson(), UsageEvent.class);
    if (lease.settled()) return e;
    // Exactly what reap.lua added to the tenant's used tokens, so Redis and Postgres agree.
    return e.withTokens(Math.toIntExact(lease.promptHold()), Math.toIntExact(lease.grantedOutput()));
  }
}
