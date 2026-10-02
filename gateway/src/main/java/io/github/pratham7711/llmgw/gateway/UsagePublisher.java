package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.Topics;
import io.github.pratham7711.llmgw.common.UsageEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publishes one usage event per forwarded request, keyed by tenant so a tenant's events stay in
 * one partition and are consumed in order.
 *
 * KafkaProducer.send() is not fully asynchronous: after a leader change it blocks the caller
 * while it refreshes metadata, for up to max.block.ms. Called on the request thread, a 1.3 s
 * broker restart held requests for seconds and filled the in-flight limit (see docs/DESIGN.md).
 * So the request thread only offers the event to a bounded queue, and one dispatcher thread
 * makes the send. A single dispatcher keeps each tenant's events in order, and it retries a send
 * that timed out waiting for metadata rather than dropping it.
 *
 * Once Kafka acknowledges an event, its lease is released. An event that never gets there (the
 * queue is full, a send fails after delivery.timeout.ms, or the process dies) is counted and
 * logged to "usage.unpublished", and its lease, which holds the same event, outlives it: the
 * reaper in metering publishes it when the lease expires.
 *
 * A request whose lease could not be settled (Redis failed at that moment) still holds its
 * reservation, so its lease is not released on acknowledgement: the reaper moves the reservation
 * into usage and republishes the event, which the sink drops as a duplicate of this one.
 */
@Component
public class UsagePublisher {

  private static final Logger log = LoggerFactory.getLogger(UsagePublisher.class);
  private static final Logger lost = LoggerFactory.getLogger("usage.unpublished");
  private static final long DRAIN_ON_SHUTDOWN_SECONDS = 10;

  private final KafkaTemplate<String, String> kafka;
  private final LeaseReleaser leases;
  private final JsonMapper json;
  private record Pending(UsageEvent event, boolean releaseLease) {}

  private final BlockingQueue<Pending> queue;
  private final Counter published;
  private final Counter failed;
  private final Thread dispatcher;
  private volatile boolean stopping;

  public UsagePublisher(KafkaTemplate<String, String> kafka, LeaseReleaser leases, JsonMapper json, MeterRegistry meters,
      @Value("${gateway.usage-queue-capacity:50000}") int capacity) {
    this.kafka = kafka;
    this.leases = leases;
    this.json = json;
    this.queue = new ArrayBlockingQueue<>(capacity);
    this.published = Counter.builder("gateway.usage.events").tag("result", "published").register(meters);
    this.failed = Counter.builder("gateway.usage.events").tag("result", "failed").register(meters);
    Gauge.builder("gateway.usage.queue", queue, BlockingQueue::size).register(meters);
    this.dispatcher = Thread.ofPlatform().name("usage-dispatcher").daemon().start(this::dispatch);
  }

  /** Never blocks. */
  public void publish(UsageEvent event) {
    publish(event, true);
  }

  /** Never blocks. With {@code releaseLease} false the lease is left for the reaper. */
  public void publish(UsageEvent event, boolean releaseLease) {
    if (!queue.offer(new Pending(event, releaseLease))) fail(event, "dispatch queue full");
  }

  private void dispatch() {
    while (!stopping || !queue.isEmpty()) {
      Pending next;
      try {
        next = queue.poll(200, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (next != null) send(next.event(), next.releaseLease());
    }
  }

  /**
   * A metadata timeout is thrown by send() itself before the record reaches the producer, so it
   * is safe to retry, and the idempotent sink would absorb a duplicate anyway. A restarting broker
   * produced exactly this on the dispatcher; without the retry that event was dropped.
   */
  private void send(UsageEvent event, boolean releaseLease) {
    String payload;
    try {
      payload = json.writeValueAsString(event);
    } catch (RuntimeException e) {
      fail(event, e.toString());
      return;
    }
    long backoffMs = 100;
    while (true) {
      try {
        kafka.send(Topics.USAGE_EVENTS, event.tenantId(), payload).whenComplete((ok, err) -> {
          if (err == null) {
            published.increment();
            if (releaseLease) leases.release(event.tenantId(), event.eventId().toString());
          } else {
            fail(event, err.toString());
          }
        });
        return;
      } catch (RuntimeException e) {
        if (stopping || !metadataTimeout(e)) {
          fail(event, e.toString());
          return;
        }
        log.warn("usage event {} waiting for Kafka metadata, retrying in {} ms", event.eventId(), backoffMs);
        if (!pause(backoffMs)) {
          fail(event, "interrupted while waiting for Kafka");
          return;
        }
        backoffMs = Math.min(backoffMs * 2, 2000);
      }
    }
  }

  private static boolean metadataTimeout(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof org.apache.kafka.common.errors.TimeoutException) return true;
    }
    return false;
  }

  private static boolean pause(long ms) {
    try {
      Thread.sleep(ms);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void fail(UsageEvent event, String reason) {
    failed.increment();
    log.error("usage event {} not published: {}", event.eventId(), reason);
    try {
      lost.error(json.writeValueAsString(event));
    } catch (RuntimeException e) {
      lost.error(event.toString());
    }
  }

  @PreDestroy
  void drain() throws InterruptedException {
    stopping = true;
    dispatcher.join(TimeUnit.SECONDS.toMillis(DRAIN_ON_SHUTDOWN_SECONDS));
    for (Pending left; (left = queue.poll()) != null; ) fail(left.event(), "gateway shut down before dispatch");
    kafka.flush();
  }
}
