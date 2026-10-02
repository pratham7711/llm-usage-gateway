package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Deletes a request's lease once Kafka has acknowledged its usage event: from then on the event
 * is durable and the lease has nothing left to protect.
 *
 * Acknowledgements arrive on the Kafka producer's I/O thread, which must never wait on Redis, so
 * they are queued and one thread deletes them in pipelined batches. A delete that fails is only
 * logged: the lease then expires and the reaper republishes an event with the same id, which the
 * metering sink drops as a duplicate.
 */
@Component
public class LeaseReleaser {

  private static final Logger log = LoggerFactory.getLogger(LeaseReleaser.class);
  private static final int BATCH = 1000;

  private record Ref(String tenantId, String leaseId) {}

  private final StringRedisTemplate redis;
  private final LinkedBlockingQueue<Ref> queue = new LinkedBlockingQueue<>();
  private final Counter released;
  private final Counter failed;
  private final Thread worker;
  private volatile boolean stopping;

  public LeaseReleaser(StringRedisTemplate redis, MeterRegistry meters) {
    this.redis = redis;
    this.released = meters.counter("gateway.leases", "result", "released");
    this.failed = meters.counter("gateway.leases", "result", "release_failed");
    meters.gauge("gateway.leases.release.queue", queue, LinkedBlockingQueue::size);
    this.worker = Thread.ofPlatform().name("lease-releaser").daemon().start(this::run);
  }

  /** Never blocks. */
  public void release(String tenantId, String leaseId) {
    queue.offer(new Ref(tenantId, leaseId));
  }

  private void run() {
    List<Ref> batch = new ArrayList<>(BATCH);
    while (!stopping || !queue.isEmpty()) {
      try {
        Ref first = queue.poll(200, TimeUnit.MILLISECONDS);
        if (first == null) continue;
        batch.add(first);
        queue.drainTo(batch, BATCH - 1);
        delete(batch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } finally {
        batch.clear();
      }
    }
  }

  private void delete(List<Ref> batch) {
    try {
      // A pipelined callback gets the raw connection, not the String wrapper, so keys go as bytes.
      redis.executePipelined((RedisCallback<Object>) c -> {
        for (Ref r : batch) {
          byte[] id = r.leaseId().getBytes(StandardCharsets.UTF_8);
          c.hashCommands().hDel(Topics.leaseKey(r.tenantId()).getBytes(StandardCharsets.UTF_8), id);
          c.zSetCommands().zRem(Topics.leaseExpiryKey(r.tenantId()).getBytes(StandardCharsets.UTF_8), id);
        }
        return null;
      });
      released.increment(batch.size());
    } catch (RuntimeException e) {
      failed.increment(batch.size());
      log.warn("could not release {} leases, the reaper will collect them: {}", batch.size(), e.toString());
    }
  }

  @PreDestroy
  void stop() throws InterruptedException {
    stopping = true;
    worker.join(TimeUnit.SECONDS.toMillis(5));
  }
}
