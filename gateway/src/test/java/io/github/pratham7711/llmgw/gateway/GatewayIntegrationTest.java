package io.github.pratham7711.llmgw.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pratham7711.llmgw.common.Topics;
import io.github.pratham7711.llmgw.common.UsageEvent;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GatewayIntegrationTest {

  @Container @ServiceConnection
  static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

  @Container @ServiceConnection
  static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

  @Container @ServiceConnection(name = "redis")
  static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

  static FakeUpstream upstream;
  static KafkaConsumer<String, String> consumer;
  static final List<UsageEvent> events = new CopyOnWriteArrayList<>();

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry r) throws Exception {
    if (upstream == null) upstream = new FakeUpstream();
    r.add("gateway.upstream-base-url", upstream::baseUrl);
    r.add("gateway.upstream-api-key", () -> "sk-upstream-test");
    // Short enough to test the stream deadline; the lease ttl must still outlast every request.
    r.add("gateway.max-stream-duration", () -> "3s");
    r.add("gateway.upstream-request-timeout", () -> "10s");
    r.add("gateway.lease-ttl", () -> "45s");
    r.add("gateway.estimate-sample-rate", () -> "1.0");
  }

  @AfterAll
  static void stop() {
    if (consumer != null) consumer.close();
    if (upstream != null) upstream.stop();
  }

  @LocalServerPort int port;
  @Autowired JsonMapper json;
  @Autowired StringRedisTemplate redisTemplate;
  @Autowired JdbcTemplate jdbc;
  @Autowired TokenCounter tokenCounter;

  private final HttpClient http = HttpClient.newHttpClient();

  @BeforeAll
  static void consume() {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumer = new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(List.of(Topics.USAGE_EVENTS));
  }

  private List<UsageEvent> eventsFor(String model) {
    for (var rec : consumer.poll(Duration.ofMillis(200))) events.add(json.readValue(rec.value(), UsageEvent.class));
    return events.stream().filter(e -> e.model().equals(model)).toList();
  }

  private HttpResponse<String> post(String key, String body) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body));
    if (key != null) b.header("Authorization", "Bearer " + key);
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> get(String key, String path) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET();
    if (key != null) b.header("Authorization", "Bearer " + key);
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  /** A tenant with no rate limit to speak of and the given quota; returns its API key. */
  private String tenant(String id, long quota) {
    jdbc.update("insert into tenant (id, name, rate_per_sec, burst, monthly_token_quota) values (?, ?, 100000, 200000, ?)",
        id, id, quota);
    jdbc.update("insert into api_key (key_hash, tenant_id, label) values (encode(sha256(convert_to(?, 'UTF8')), 'hex'), ?, 'test')",
        "sk-" + id, id);
    return "sk-" + id;
  }

  private long redisLong(String key) {
    String v = redisTemplate.opsForValue().get(key);
    return v == null ? 0 : Long.parseLong(v);
  }

  private static String chat(String model, boolean stream, String extra) {
    return "{\"model\":\"" + model + "\",\"stream\":" + stream
        + ",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]" + extra + "}";
  }

  @Test
  void rejectsMissingOrUnknownKey() throws Exception {
    assertThat(post(null, chat("m-auth", false, "")).statusCode()).isEqualTo(401);
    HttpResponse<String> bad = post("sk-not-a-key", chat("m-auth", false, ""));
    assertThat(bad.statusCode()).isEqualTo(401);
    assertThat(bad.body()).contains("invalid_api_key");
  }

  @Test
  void relaysCompletionAndPublishesOneUsageEvent() throws Exception {
    HttpResponse<String> r = post("sk-dev-lt-01", chat("m-plain", false, ""));
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(r.body()).contains("fake reply");
    assertThat(upstream.lastAuth.get()).isEqualTo("Bearer sk-upstream-test");

    await().atMost(Duration.ofSeconds(20)).until(() -> !eventsFor("m-plain").isEmpty());
    UsageEvent e = eventsFor("m-plain").getFirst();
    assertThat(e.tenantId()).isEqualTo("lt-01");
    assertThat(e.tokensIn()).isEqualTo(FakeUpstream.PROMPT_TOKENS);
    assertThat(e.tokensOut()).isEqualTo(FakeUpstream.COMPLETION_TOKENS);
    assertThat(e.status()).isEqualTo(200);
    assertThat(e.streamed()).isFalse();
    assertThat(eventsFor("m-plain")).hasSize(1);
  }

  @Test
  void streamsChunksAndHidesTheUsageChunkTheGatewayAskedFor() throws Exception {
    HttpResponse<String> r = post("sk-dev-lt-02", chat("m-stream", true, ""));
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(r.headers().firstValue("Content-Type").orElse("")).startsWith("text/event-stream");
    assertThat(r.body()).contains("fake ", "streamed ", "reply", "data: [DONE]");
    assertThat(r.body()).doesNotContain("\"usage\"");
    assertThat(r.body()).doesNotContain("\n\n\n");
    JsonNode sent = json.readTree(upstream.lastBody.get());
    assertThat(sent.path("stream_options").path("include_usage").asBoolean()).isTrue();

    await().atMost(Duration.ofSeconds(20)).until(() -> !eventsFor("m-stream").isEmpty());
    UsageEvent e = eventsFor("m-stream").getFirst();
    assertThat(e.streamed()).isTrue();
    assertThat(e.tokensIn()).isEqualTo(FakeUpstream.PROMPT_TOKENS);
    assertThat(e.tokensOut()).isEqualTo(FakeUpstream.COMPLETION_TOKENS);
  }

  @Test
  void keepsTheUsageChunkWhenTheClientAskedForIt() throws Exception {
    HttpResponse<String> r = post("sk-dev-lt-03", chat("m-stream-usage", true, ",\"stream_options\":{\"include_usage\":true}"));
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(r.body()).contains("\"usage\"");
  }

  @Test
  void deliversEachEventAsItArrivesInsteadOfBufferingTheStream() throws Exception {
    HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer sk-dev-lt-07")
        .POST(HttpRequest.BodyPublishers.ofString(chat("slow-stream-ttfb", true, "")))
        .build();
    long t0 = System.nanoTime();
    HttpResponse<java.io.InputStream> r = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
    try (java.io.BufferedReader lines = new java.io.BufferedReader(new java.io.InputStreamReader(r.body()))) {
      assertThat(lines.readLine()).startsWith("data: ");
      long firstEventMs = (System.nanoTime() - t0) / 1_000_000;
      while (lines.readLine() != null) { }
      long lastEventMs = (System.nanoTime() - t0) / 1_000_000;
      // The upstream spaces 8 events 250 ms apart. A buffered relay delivers them all at the end.
      assertThat(lastEventMs - firstEventMs).isGreaterThan(1500);
    }
  }

  @Test
  void billsTheWholeCompletionWhenTheClientDisconnectsMidStream() throws Exception {
    HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer sk-dev-lt-06")
        .POST(HttpRequest.BodyPublishers.ofString(chat("slow-stream", true, "")))
        .build();
    HttpResponse<java.io.InputStream> r = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
    assertThat(r.statusCode()).isEqualTo(200);
    try (java.io.InputStream in = r.body()) {
      assertThat(in.read(new byte[64])).isPositive();
    }

    // The gateway notices the closed socket on a later write, keeps draining upstream, and still
    // bills the usage reported in the final chunk.
    await().atMost(Duration.ofSeconds(20)).until(() -> !eventsFor("slow-stream").isEmpty());
    UsageEvent e = eventsFor("slow-stream").getFirst();
    assertThat(e.status()).isEqualTo(499);
    assertThat(e.tokensIn()).isEqualTo(FakeUpstream.PROMPT_TOKENS);
    assertThat(e.tokensOut()).isEqualTo(FakeUpstream.COMPLETION_TOKENS);
  }

  @Test
  void rateLimitsATenantPastItsBurst() throws Exception {
    int admitted = 0;
    int limited = 0;
    String retryAfter = null;
    for (int i = 0; i < 16; i++) {
      HttpResponse<String> r = post("sk-dev-hot", chat("m-hot", false, ""));
      if (r.statusCode() == 200) admitted++;
      if (r.statusCode() == 429) {
        limited++;
        retryAfter = r.headers().firstValue("Retry-After").orElse(null);
        assertThat(r.body()).contains("rate_limit_exceeded");
      }
    }
    assertThat(admitted).isGreaterThanOrEqualTo(10);
    assertThat(limited).isGreaterThanOrEqualTo(1);
    assertThat(retryAfter).isNotNull();
    // Rejected requests are never forwarded, so they are never billed.
    final int forwarded = admitted;
    await().atMost(Duration.ofSeconds(20)).until(() -> eventsFor("m-hot").size() == forwarded);
  }

  @Test
  void failsClosedAndFastWhileRedisIsUnreachable() throws Exception {
    var docker = redis.getDockerClient();
    docker.pauseContainerCmd(redis.getContainerId()).exec();
    try {
      long started = System.nanoTime();
      HttpResponse<String> r = post("sk-dev-demo", chat("m-redis-down", false, ""));
      long tookMs = (System.nanoTime() - started) / 1_000_000;
      assertThat(r.statusCode()).isEqualTo(503);
      assertThat(r.headers().firstValue("Retry-After")).contains("1");
      assertThat(r.body()).contains("admission_unavailable");
      assertThat(tookMs).isLessThan(3000);
    } finally {
      docker.unpauseContainerCmd(redis.getContainerId()).exec();
    }
    await().atMost(Duration.ofSeconds(20)).until(() -> post("sk-dev-demo", chat("m-redis-back", false, "")).statusCode() == 200);
    // Nothing was forwarded while admission was down, so nothing was billed for it.
    assertThat(eventsFor("m-redis-down")).isEmpty();
  }

  @Test
  void refusesATenantOverItsMonthlyQuota() throws Exception {
    redisTemplate.opsForValue().set(Topics.quotaKey("capped", Topics.month(Instant.now())), "5000");
    HttpResponse<String> r = post("sk-dev-capped", chat("m-capped", false, ""));
    assertThat(r.statusCode()).isEqualTo(429);
    assertThat(r.body()).contains("insufficient_quota");
  }

  @Test
  void relaysUpstreamErrorsAndRecordsThemWithZeroTokens() throws Exception {
    HttpResponse<String> r = post("sk-dev-lt-04", chat("fail-503", false, ""));
    assertThat(r.statusCode()).isEqualTo(503);
    await().atMost(Duration.ofSeconds(20)).until(() -> !eventsFor("fail-503").isEmpty());
    UsageEvent e = eventsFor("fail-503").getFirst();
    assertThat(e.status()).isEqualTo(503);
    assertThat(e.totalTokens()).isZero();
  }

  @Test
  void rejectsMalformedBodies() throws Exception {
    assertThat(post("sk-dev-lt-05", "not json").statusCode()).isEqualTo(400);
    assertThat(post("sk-dev-lt-05", "{\"messages\":[]}").statusCode()).isEqualTo(400);
    JsonNode err = json.readTree(post("sk-dev-lt-05", "[]").body());
    assertThat(err.has("error")).isTrue();
  }
  @Test
  void concurrentRequestsNeverSpendPastTheQuota() throws Exception {
    long quota = 3_000;
    String key = tenant("race-it", quota);
    // Every request generates its full max_tokens, the worst case for a quota.
    String body = chat("fill-max-race", false, ",\"max_tokens\":40");
    List<Future<HttpResponse<String>>> calls = new ArrayList<>();
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < 200; i++) calls.add(pool.submit(() -> post(key, body)));
    }
    int ok = 0;
    int refused = 0;
    for (Future<HttpResponse<String>> f : calls) {
      HttpResponse<String> r = f.get();
      if (r.statusCode() == 200) ok++;
      else if (r.statusCode() == 429 && (r.body().contains("insufficient_quota") || r.body().contains("quota_reserved"))) refused++;
    }
    assertThat(refused).isPositive();

    String month = Topics.month(Instant.now());
    final int admitted = ok;
    await().atMost(Duration.ofSeconds(20)).until(() -> eventsFor("fill-max-race").size() == admitted
        && redisLong(Topics.heldKey("race-it", month)) == 0
        && redisTemplate.opsForHash().size(Topics.leaseKey("race-it")) == 0);
    List<UsageEvent> billed = eventsFor("fill-max-race");
    long used = redisLong(Topics.quotaKey("race-it", month));
    assertThat(billed.stream().mapToLong(UsageEvent::totalTokens).sum()).isEqualTo(used);
    assertThat(used).isLessThanOrEqualTo(quota);
    // Hard, but not wasteful: nearly the whole quota is spent.
    assertThat(used).isGreaterThan(quota * 9 / 10);
    // Near the end the provider was told to generate less than the client asked for.
    assertThat(billed).anyMatch(e -> e.tokensOut() < 40);
  }

  @Test
  void capsTheOutputSentUpstreamToWhatTheQuotaCanStillPayFor() throws Exception {
    String key = tenant("grant-it", 100);
    HttpResponse<String> r = post(key, chat("fill-max-grant", false, ",\"max_tokens\":500"));
    assertThat(r.statusCode()).isEqualTo(200);
    int promptHold = (int) Math.ceil(FakeUpstream.HI_PROMPT_TOKENS * 1.10);
    assertThat(upstream.lastMaxTokens.get()).isEqualTo(100 - promptHold);
    assertThat(r.headers().firstValue(ChatProxyController.GRANTED_MAX_TOKENS)).contains(String.valueOf(100 - promptHold));

    String month = Topics.month(Instant.now());
    await().atMost(Duration.ofSeconds(20)).until(() -> redisLong(Topics.heldKey("grant-it", month)) == 0);
    assertThat(redisLong(Topics.quotaKey("grant-it", month))).isEqualTo(FakeUpstream.HI_PROMPT_TOKENS + 100 - promptHold);
    // One token left: less than any prompt, so the next request is refused before it is forwarded.
    HttpResponse<String> next = post(key, chat("fill-max-grant", false, ",\"max_tokens\":500"));
    assertThat(next.statusCode()).isEqualTo(429);
    assertThat(next.body()).contains("insufficient_quota");
  }

  @Test
  void tellsAClientToRetryWhenTheQuotaIsOnlyReservedByRequestsInFlight() throws Exception {
    String key = tenant("reserved-it", 1_000);
    String month = Topics.month(Instant.now());
    // Another request is in flight and holds most of the quota.
    redisTemplate.opsForValue().set(Topics.heldKey("reserved-it", month), "995");
    HttpResponse<String> r = post(key, chat("m-reserved", false, ",\"max_tokens\":50"));
    assertThat(r.statusCode()).isEqualTo(429);
    assertThat(r.body()).contains("quota_reserved");
    assertThat(r.headers().firstValue("Retry-After")).contains("1");
    // Once it settles for less than it reserved, the same request fits.
    redisTemplate.opsForValue().set(Topics.heldKey("reserved-it", month), "0");
    assertThat(post(key, chat("m-reserved", false, ",\"max_tokens\":50")).statusCode()).isEqualTo(200);
  }

  @Test
  void settlingFreesTheReservationAndKafkasAckDeletesTheLease() throws Exception {
    String key = tenant("lease-it", 1_000_000);
    assertThat(post(key, chat("m-lease", false, ",\"max_tokens\":100")).statusCode()).isEqualTo(200);
    String month = Topics.month(Instant.now());
    await().atMost(Duration.ofSeconds(20)).until(() -> !eventsFor("m-lease").isEmpty()
        && redisTemplate.opsForHash().size(Topics.leaseKey("lease-it")) == 0);
    assertThat(redisLong(Topics.heldKey("lease-it", month))).isZero();
    assertThat(redisLong(Topics.quotaKey("lease-it", month))).isEqualTo(FakeUpstream.PROMPT_TOKENS + FakeUpstream.COMPLETION_TOKENS);
    assertThat(redisTemplate.opsForZSet().size(Topics.leaseExpiryKey("lease-it"))).isZero();

    UsageEvent e = eventsFor("m-lease").getFirst();
    // The provider saw the event id as X-Request-Id, so its log reconciles against billing.
    assertThat(upstream.requestIds).contains(e.eventId().toString());
    assertThat(e.source()).isEqualTo(UsageEvent.SOURCE_PROVIDER);
    assertThat(e.estTokensIn()).isEqualTo(FakeUpstream.HI_PROMPT_TOKENS);
  }

  @Test
  void billsAStreamThatEndsWithoutUsageFromTheTextItCarried() throws Exception {
    HttpResponse<String> r = post("sk-dev-lt-08", chat("cut-stream", true, ""));
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(r.body()).contains("fake ", "streamed ", "reply");

    await().atMost(Duration.ofSeconds(20)).until(() -> !eventsFor("cut-stream").isEmpty());
    UsageEvent e = eventsFor("cut-stream").getFirst();
    assertThat(e.source()).isEqualTo(UsageEvent.SOURCE_ESTIMATED);
    assertThat(e.tokensIn()).isEqualTo(FakeUpstream.HI_PROMPT_TOKENS);
    assertThat(e.tokensOut()).isEqualTo(tokenCounter.text("cut-stream", "fake streamed reply")).isPositive();
  }

  @Test
  void cutsAStreamAtTheMaximumDurationAndBillsWhatItCarried() throws Exception {
    long t0 = System.nanoTime();
    HttpResponse<String> r = post("sk-dev-lt-09", chat("endless-stream", true, ""));
    long tookMs = (System.nanoTime() - t0) / 1_000_000;
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(r.body()).contains("more ");
    assertThat(tookMs).isBetween(2_900L, 10_000L);

    await().atMost(Duration.ofSeconds(20)).until(() -> !eventsFor("endless-stream").isEmpty());
    UsageEvent e = eventsFor("endless-stream").getFirst();
    assertThat(e.status()).isEqualTo(504);
    assertThat(e.source()).isEqualTo(UsageEvent.SOURCE_ESTIMATED);
    assertThat(e.tokensOut()).isPositive();
  }

  @Test
  void reportsLiveUsageToTheTenantItself() throws Exception {
    String key = tenant("usage-it", 10_000);
    assertThat(post(key, chat("m-usage", false, "")).statusCode()).isEqualTo(200);
    await().atMost(Duration.ofSeconds(20))
        .until(() -> redisLong(Topics.heldKey("usage-it", Topics.month(Instant.now()))) == 0);

    HttpResponse<String> r = get(key, "/v1/usage");
    assertThat(r.statusCode()).isEqualTo(200);
    JsonNode u = json.readTree(r.body());
    assertThat(u.path("tenant").asString()).isEqualTo("usage-it");
    assertThat(u.path("quota_tokens").asLong()).isEqualTo(10_000);
    long used = FakeUpstream.PROMPT_TOKENS + FakeUpstream.COMPLETION_TOKENS;
    assertThat(u.path("live").path("used_tokens").asLong()).isEqualTo(used);
    assertThat(u.path("live").path("held_tokens").asLong()).isZero();
    assertThat(u.path("live").path("remaining_tokens").asLong()).isEqualTo(10_000 - used);
    assertThat(u.has("billed")).isTrue();

    assertThat(get(null, "/v1/usage").statusCode()).isEqualTo(401);
    assertThat(get(key, "/v1/usage?month=oct").statusCode()).isEqualTo(400);
  }
}
