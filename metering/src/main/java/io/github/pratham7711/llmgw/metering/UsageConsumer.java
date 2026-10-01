package io.github.pratham7711.llmgw.metering;

import io.github.pratham7711.llmgw.common.Topics;
import io.github.pratham7711.llmgw.common.UsageEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Batch listener. Offsets are committed only after the listener returns, i.e. after the Postgres
 * transaction has committed and every poison record has been acknowledged by the dead-letter
 * topic. Delivery is at-least-once; the event-id primary key turns it into exactly-once billing.
 * Any other failure (database down, DLT unavailable) throws, and the container retries the whole
 * batch with backoff for as long as it takes rather than skipping billable events.
 */
@Component
public class UsageConsumer {

  private static final Logger log = LoggerFactory.getLogger(UsageConsumer.class);

  private final JsonMapper json;
  private final UsageStore store;
  private final QuotaCache quota;
  private final KafkaTemplate<String, String> kafka;
  private final Counter inserted;
  private final Counter duplicates;
  private final Counter deadLettered;
  private final Timer batchTimer;

  public UsageConsumer(JsonMapper json, UsageStore store, QuotaCache quota,
      KafkaTemplate<String, String> kafka, MeterRegistry meters) {
    this.json = json;
    this.store = store;
    this.quota = quota;
    this.kafka = kafka;
    this.inserted = meters.counter("metering.events", "result", "inserted");
    this.duplicates = meters.counter("metering.events", "result", "duplicate");
    this.deadLettered = meters.counter("metering.events", "result", "dead_lettered");
    this.batchTimer = Timer.builder("metering.batch").publishPercentileHistogram().register(meters);
  }

  @KafkaListener(topics = Topics.USAGE_EVENTS, groupId = "${metering.group-id:metering}", batch = "true",
      concurrency = "${metering.concurrency:3}")
  public void onBatch(List<ConsumerRecord<String, String>> records) throws Exception {
    long t0 = System.nanoTime();
    List<UsageStore.Located> good = new ArrayList<>(records.size());
    List<CompletableFuture<?>> dlt = new ArrayList<>();
    for (ConsumerRecord<String, String> r : records) {
      String reason;
      try {
        UsageEvent e = json.readValue(r.value(), UsageEvent.class);
        reason = e.validationError();
        if (reason == null) {
          good.add(new UsageStore.Located(e, r.partition(), r.offset()));
          continue;
        }
      } catch (JacksonException | IllegalArgumentException ex) {
        reason = "unparseable: " + ex.getClass().getSimpleName();
      }
      dlt.add(deadLetter(r, reason));
    }

    for (CompletableFuture<?> f : dlt) f.get(30, TimeUnit.SECONDS);
    UsageStore.Result result = store.apply(good);
    quota.writeBack(result.totals());

    inserted.increment(result.inserted());
    duplicates.increment(result.duplicates());
    deadLettered.increment(dlt.size());
    batchTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
    if (result.duplicates() > 0) {
      log.info("batch of {}: {} inserted, {} duplicates ignored", records.size(), result.inserted(), result.duplicates());
    }
  }

  private CompletableFuture<?> deadLetter(ConsumerRecord<String, String> r, String reason) {
    log.warn("dead-lettering {}-{}@{}: {}", r.topic(), r.partition(), r.offset(), reason);
    ProducerRecord<String, String> out = new ProducerRecord<>(Topics.USAGE_EVENTS_DLT, r.key(), r.value());
    out.headers().add("dlt-reason", reason.getBytes(StandardCharsets.UTF_8));
    out.headers().add("dlt-source", (r.topic() + "-" + r.partition() + "@" + r.offset()).getBytes(StandardCharsets.UTF_8));
    return kafka.send(out);
  }
}
