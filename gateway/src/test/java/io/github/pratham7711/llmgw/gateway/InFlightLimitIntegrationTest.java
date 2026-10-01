package io.github.pratham7711.llmgw.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "gateway.max-in-flight=1")
class InFlightLimitIntegrationTest {

  @Container @ServiceConnection
  static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

  @Container @ServiceConnection
  static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

  @Container @ServiceConnection(name = "redis")
  static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

  static FakeUpstream upstream;

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry r) throws Exception {
    if (upstream == null) upstream = new FakeUpstream();
    r.add("gateway.upstream-base-url", upstream::baseUrl);
  }

  @AfterAll
  static void stop() {
    if (upstream != null) upstream.stop();
  }

  @LocalServerPort int port;
  private final HttpClient http = HttpClient.newHttpClient();

  private HttpRequest request(String model, boolean stream) {
    return HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer sk-dev-lt-20")
        .POST(HttpRequest.BodyPublishers.ofString("{\"model\":\"" + model + "\",\"stream\":" + stream
            + ",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
        .build();
  }

  @Test
  void shedsLoadWhileAStreamHoldsTheOnlyPermitAndRecoversAfterIt() throws Exception {
    // A slow stream (8 chunks, 250 ms apart) holds the single permit for about 2 s.
    CompletableFuture<HttpResponse<InputStream>> held =
        http.sendAsync(request("slow-stream", true), HttpResponse.BodyHandlers.ofInputStream());
    HttpResponse<InputStream> first = held.get();
    assertThat(first.statusCode()).isEqualTo(200);

    HttpResponse<String> shed = http.send(request("m-limit", false), HttpResponse.BodyHandlers.ofString());
    assertThat(shed.statusCode()).isEqualTo(503);
    assertThat(shed.headers().firstValue("Retry-After")).contains("1");
    assertThat(shed.body()).contains("overloaded");

    try (InputStream in = first.body()) {
      in.readAllBytes();
    }
    // Once the stream completes, its permit is released and the next request is admitted.
    await().atMost(Duration.ofSeconds(10)).until(() ->
        http.send(request("m-limit", false), HttpResponse.BodyHandlers.ofString()).statusCode() == 200);
  }

  @Test
  void aDrainThatOutlivesItsClientKeepsThePermitUntilUpstreamFinishes() throws Exception {
    HttpResponse<InputStream> first = http.send(request("slow-stream", true), HttpResponse.BodyHandlers.ofInputStream());
    try (InputStream in = first.body()) {
      assertThat(in.read(new byte[64])).isPositive();
    }
    // By now the gateway has failed a write and finished the response, but upstream still has
    // about a second of events left to drain on the gateway's side.
    Thread.sleep(800);
    assertThat(http.send(request("m-limit", false), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);

    await().atMost(Duration.ofSeconds(10)).until(() ->
        http.send(request("m-limit", false), HttpResponse.BodyHandlers.ofString()).statusCode() == 200);
  }

  @Test
  void healthEndpointsAreNeverShed() throws Exception {
    HttpResponse<String> r = http.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health")).build(),
        HttpResponse.BodyHandlers.ofString());
    assertThat(r.statusCode()).isEqualTo(200);
  }
}
