package io.github.pratham7711.llmgw.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.pratham7711.llmgw.common.UsageEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

class UsagePublisherTest {

  private final JsonMapper json = JsonMapper.builder().build();
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

  private static UsageEvent event(String tenant, int n) {
    return new UsageEvent(UUID.randomUUID(), tenant, "m-" + n, 1, 1, 1, 200, false, Instant.now());
  }

  private double failed() {
    return meters.get("gateway.usage.events").tag("result", "failed").counter().count();
  }

  @Test
  @SuppressWarnings("unchecked")
  void publishReturnsAtOnceWhileTheProducerIsBlockedAndKeepsOrder() throws Exception {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    List<String> sent = new CopyOnWriteArrayList<>();
    CountDownLatch all = new CountDownLatch(3);
    // Each send blocks the way KafkaProducer.send() does while it waits for metadata.
    when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(call -> {
      Thread.sleep(500);
      sent.add(json.readTree(call.<String>getArgument(2)).path("model").asString());
      all.countDown();
      return CompletableFuture.completedFuture(null);
    });
    UsagePublisher publisher = new UsagePublisher(kafka, json, meters, 100);

    long started = System.nanoTime();
    for (int i = 0; i < 3; i++) publisher.publish(event("t1", i));
    long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertThat(tookMs).isLessThan(100);
    assertThat(all.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(sent).containsExactly("m-0", "m-1", "m-2");
    assertThat(failed()).isZero();
  }

  @Test
  @SuppressWarnings("unchecked")
  void retriesWhenKafkaHasNoMetadataYetInsteadOfDroppingTheEvent() throws Exception {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    AtomicInteger attempts = new AtomicInteger();
    CountDownLatch sent = new CountDownLatch(1);
    // What KafkaTemplate throws while a restarted broker is not yet serving metadata.
    when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(call -> {
      if (attempts.incrementAndGet() <= 2) {
        throw new KafkaException("Send failed",
            new org.apache.kafka.common.errors.TimeoutException("Topic usage-events not present in metadata after 5000 ms."));
      }
      sent.countDown();
      return CompletableFuture.completedFuture(null);
    });
    UsagePublisher publisher = new UsagePublisher(kafka, json, meters, 100);

    publisher.publish(event("t1", 0));

    assertThat(sent.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(attempts.get()).isEqualTo(3);
    assertThat(failed()).isZero();
  }

  @Test
  @SuppressWarnings("unchecked")
  void aFullQueueFailsTheEventInsteadOfBlockingTheCaller() throws Exception {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    CountDownLatch release = new CountDownLatch(1);
    when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(call -> {
      release.await(10, TimeUnit.SECONDS);
      return CompletableFuture.completedFuture(null);
    });
    UsagePublisher publisher = new UsagePublisher(kafka, json, meters, 1);

    long started = System.nanoTime();
    for (int i = 0; i < 6; i++) {
      publisher.publish(event("t1", i));
      Thread.sleep(20);
    }
    long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    release.countDown();

    // One event is held by the blocked dispatcher, one fits in the queue, the rest fail fast.
    assertThat(tookMs).isLessThan(1000);
    assertThat(failed()).isEqualTo(4);
  }
}
