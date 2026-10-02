package io.github.pratham7711.llmgw.metering;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pratham7711.llmgw.common.Topics;
import io.github.pratham7711.llmgw.common.UsageEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class MeteringIntegrationTest {

  @Container @ServiceConnection
  static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

  @Container @ServiceConnection
  static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

  @Container @ServiceConnection(name = "redis")
  static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

  @Autowired KafkaTemplate<String, String> producer;
  @Autowired JdbcTemplate jdbc;
  @Autowired StringRedisTemplate redisTemplate;
  @Autowired JsonMapper json;
  @Autowired LeaseReaper reaper;
  @Autowired PriceBook prices;

  private UsageEvent event(String tenant, int in, int out, int status, Instant at) {
    return new UsageEvent(UUID.randomUUID(), tenant, "m", in, out, 12, status, false, at);
  }

  private void send(UsageEvent e) throws Exception {
    producer.send(Topics.USAGE_EVENTS, e.tenantId(), json.writeValueAsString(e)).get();
  }

  private long count(String sql, Object... args) {
    Long n = jdbc.queryForObject(sql, Long.class, args);
    return n == null ? 0 : n;
  }

  @Test
  void redeliveredEventsAreBilledExactlyOnce() throws Exception {
    Instant now = Instant.now();
    UsageEvent once = event("lt-10", 100, 50, 200, now);
    UsageEvent other = event("lt-10", 10, 5, 200, now);
    send(once);
    send(once);
    send(once);
    send(other);

    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(count("select count(*) from usage_event where tenant_id = 'lt-10'")).isEqualTo(2);
      assertThat(count("select coalesce(sum(requests),0) from usage_rollup_minute where tenant_id = 'lt-10'")).isEqualTo(2);
      assertThat(count("select tokens from tenant_month_usage where tenant_id = 'lt-10' and month = ?", Topics.month(now)))
          .isEqualTo(165);
      assertThat(redisTemplate.opsForValue().get(Topics.quotaKey("lt-10", Topics.month(now)))).isEqualTo("165");
    });
  }

  @Test
  void rollsUpPerMinuteAndCountsErrors() throws Exception {
    Instant minute = Instant.now().truncatedTo(ChronoUnit.MINUTES);
    send(event("lt-11", 1, 2, 200, minute.plusSeconds(5)));
    send(event("lt-11", 3, 4, 200, minute.plusSeconds(40)));
    send(event("lt-11", 0, 0, 503, minute.plusSeconds(50)));

    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      var row = jdbc.queryForMap(
          "select requests, errors, tokens_in, tokens_out from usage_rollup_minute where tenant_id = 'lt-11'");
      assertThat(row.get("requests")).isEqualTo(3L);
      assertThat(row.get("errors")).isEqualTo(1L);
      assertThat(row.get("tokens_in")).isEqualTo(4L);
      assertThat(row.get("tokens_out")).isEqualTo(6L);
    });
  }

  @Test
  void poisonRecordsGoToTheDeadLetterTopicWithoutBlockingGoodOnes() throws Exception {
    producer.send(Topics.USAGE_EVENTS, "lt-12", "this is not json").get();
    send(new UsageEvent(UUID.randomUUID(), "lt-12", "m", -1, 0, 1, 200, false, Instant.now()));
    send(event("lt-12", 7, 7, 200, Instant.now()));

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(count("select count(*) from usage_event where tenant_id = 'lt-12'")).isEqualTo(1));

    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-reader-" + UUID.randomUUID());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<ConsumerRecord<String, String>> dead = new ArrayList<>();
    try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
      c.subscribe(List.of(Topics.USAGE_EVENTS_DLT));
      await().atMost(Duration.ofSeconds(30)).until(() -> {
        c.poll(Duration.ofMillis(300)).forEach(r -> { if ("lt-12".equals(r.key())) dead.add(r); });
        return dead.size() >= 2;
      });
    }
    List<String> reasons = dead.stream()
        .map(r -> new String(r.headers().lastHeader("dlt-reason").value(), StandardCharsets.UTF_8)).toList();
    assertThat(reasons).anyMatch(s -> s.startsWith("unparseable"));
    assertThat(reasons).contains("negative token count");
  }
  @Test
  void reaperBillsAnAbandonedLeaseAtItsReservationAndRepublishesASettledOneOnce() throws Exception {
    String t = "lt-13";
    Instant now = Instant.now();
    String month = Topics.month(now);
    // A request the gateway admitted and never settled (it crashed): 9 prompt tokens and 40 output
    // tokens reserved, from a local prompt count of 8.
    UUID abandoned = UUID.randomUUID();
    UsageEvent template = new UsageEvent(abandoned, t, "mock-small", 8, 0, 0, 0, false, now,
        UsageEvent.SOURCE_LEASE_EXPIRED, 8, null);
    redisTemplate.opsForHash().put(Topics.leaseKey(t), abandoned.toString(), "H|49|9|" + month + "|" + json.writeValueAsString(template));
    redisTemplate.opsForZSet().add(Topics.leaseExpiryKey(t), abandoned.toString(), 0);
    redisTemplate.opsForValue().increment(Topics.heldKey(t, month), 49);
    // A request that settled (its usage is already counted) and whose event Kafka acknowledged, but
    // the gateway died before deleting the lease: the reaper publishes it again.
    UUID settled = UUID.randomUUID();
    UsageEvent real = new UsageEvent(settled, t, "mock-small", 11, 7, 5, 200, false, now,
        UsageEvent.SOURCE_PROVIDER, 8, null);
    redisTemplate.opsForHash().put(Topics.leaseKey(t), settled.toString(), "S|" + json.writeValueAsString(real));
    redisTemplate.opsForZSet().add(Topics.leaseExpiryKey(t), settled.toString(), 0);
    redisTemplate.opsForValue().increment(Topics.quotaKey(t, month), 18);
    send(real);
    // A request still running: its lease has not expired and must be left alone.
    UUID live = UUID.randomUUID();
    redisTemplate.opsForHash().put(Topics.leaseKey(t), live.toString(), "H|49|9|" + month + "|" + json.writeValueAsString(template));
    redisTemplate.opsForZSet().add(Topics.leaseExpiryKey(t), live.toString(), System.currentTimeMillis() + 3_600_000);
    redisTemplate.opsForValue().increment(Topics.heldKey(t, month), 49);

    // The scheduled pass may run at the same time; claiming is atomic, so that is safe.
    reaper.reapAll();

    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(count("select count(*) from usage_event where tenant_id = ?", t)).isEqualTo(2);
      assertThat(count("select tokens from tenant_month_usage where tenant_id = ? and month = ?", t, month)).isEqualTo(49 + 18);
    });
    var row = jdbc.queryForMap("select tokens_in, tokens_out, status, usage_source, est_tokens_in, cost_nano_usd "
        + "from usage_event where event_id = ?", abandoned);
    assertThat(row.get("tokens_in")).isEqualTo(9);
    assertThat(row.get("tokens_out")).isEqualTo(40);
    assertThat(row.get("status")).isEqualTo(0);
    assertThat(row.get("usage_source")).isEqualTo(UsageEvent.SOURCE_LEASE_EXPIRED);
    assertThat(row.get("est_tokens_in")).isEqualTo(8);
    // mock-small is priced like gpt-4o-mini: $0.15 in and $0.60 out per million tokens.
    assertThat(row.get("cost_nano_usd")).isEqualTo(9L * 150 + 40L * 600);

    assertThat(redisTemplate.opsForValue().get(Topics.heldKey(t, month))).isEqualTo("49");
    assertThat(redisTemplate.opsForValue().get(Topics.quotaKey(t, month))).isEqualTo("67");
    assertThat(redisTemplate.opsForHash().keys(Topics.leaseKey(t))).containsExactly(live.toString());
    await().atMost(Duration.ofSeconds(30))
        .until(() -> redisTemplate.opsForList().size(Topics.reapedKey(t)) == 0);

    // Running it again finds nothing more to bill.
    reaper.reapAll();
    assertThat(count("select count(*) from usage_event where tenant_id = ?", t)).isEqualTo(2);
  }

  @Test
  void pricesEachRequestAtThePriceInForceWhenItOccurred() throws Exception {
    jdbc.update("insert into model_price values ('m-priced', '2026-01-01', 1.00, 2.00), ('m-priced', '2026-06-01', 3.00, 4.00)");
    prices.reload();
    Instant march = Instant.parse("2026-03-10T10:00:00Z");
    Instant july = Instant.parse("2026-07-10T10:00:00Z");
    send(new UsageEvent(UUID.randomUUID(), "lt-14", "m-priced", 100, 10, 1, 200, false, march));
    send(new UsageEvent(UUID.randomUUID(), "lt-14", "m-priced", 100, 10, 1, 200, false, july));

    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(count("select cost_nano_usd from tenant_month_usage where tenant_id = 'lt-14' and month = '2026-03'"))
          .isEqualTo(100L * 1_000 + 10L * 2_000);
      assertThat(count("select cost_nano_usd from tenant_month_usage where tenant_id = 'lt-14' and month = '2026-07'"))
          .isEqualTo(100L * 3_000 + 10L * 4_000);
      assertThat(count("select sum(cost_nano_usd) from usage_rollup_minute where tenant_id = 'lt-14'"))
          .isEqualTo(120_000L + 340_000L);
    });
  }
}
