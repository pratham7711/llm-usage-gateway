package io.github.pratham7711.llmgw.gateway;

import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Relays one upstream SSE stream to one client and reports the token usage it carried.
 *
 * The upstream is read on its own virtual thread, not on the request's streaming task. When a
 * client disconnects (or the async request times out) Spring cancels that task with an interrupt,
 * and an interrupted read would abort the upstream stream before its final usage chunk: the
 * provider bills the whole completion either way, so the tenant would be under-billed. Here the
 * interrupt only stops the client-facing writer; the drain finishes on its own thread and bills
 * the full usage with status 499.
 *
 * Some streams end without a usage chunk: the upstream connection drops, the stream runs past
 * the gateway's maximum duration and is cut, or the provider does not report usage at all. The
 * provider has still generated, and billed, what was streamed, so the relay keeps the raw chunks
 * and the gateway counts their text itself. Keeping the lines costs memory but no parsing on the
 * hot path; past {@link #MAX_BUFFERED_CHARS} the rest is extrapolated from what was kept.
 *
 * Lines pass through a small bounded queue, so a slow client still applies backpressure upstream.
 */
final class SseRelay {

  /** What one stream carried, reported once it has been fully read (or abandoned). */
  record StreamResult(int status, boolean usageReported, int promptTokens, int completionTokens,
      Supplier<String> outputText, double unbufferedFactor) {}

  interface Billing {
    void bill(StreamResult result);
  }

  static final int MAX_BUFFERED_CHARS = 512 * 1024;
  private static final String END = new String("end-of-stream");
  private static final long WRITER_GRACE_SECONDS = 30;

  private final BlockingQueue<String> lines = new ArrayBlockingQueue<>(256);
  private final AtomicBoolean clientGone = new AtomicBoolean();
  private final AtomicBoolean cut = new AtomicBoolean();
  /** Orders the watchdog's interrupt against the drain finishing (see drain()). */
  private final ReentrantLock deadlineLock = new ReentrantLock();
  private boolean drained;
  private final CountDownLatch writerDone = new CountDownLatch(1);
  private final List<String> payloads = new ArrayList<>();
  private long bufferedChars;
  private long droppedChars;
  private final InputStream upstream;
  private final boolean clientWantsUsage;
  private final JsonMapper json;
  private final Billing billing;
  private final Runnable onDrained;
  private final ScheduledExecutorService watchdog;
  private final Duration maxDuration;

  SseRelay(InputStream upstream, boolean clientWantsUsage, JsonMapper json, Billing billing, Runnable onDrained,
      ScheduledExecutorService watchdog, Duration maxDuration) {
    this.upstream = upstream;
    this.clientWantsUsage = clientWantsUsage;
    this.json = json;
    this.billing = billing;
    this.onDrained = onDrained;
    this.watchdog = watchdog;
    this.maxDuration = maxDuration;
  }

  /**
   * Runs on the streaming task. Spring hands the body a StreamUtils.NonFlushingOutputStream, so
   * out.flush() is a no-op and every event would sit in the servlet buffer until the stream ended;
   * each event is pushed with servletResponse.flushBuffer() instead.
   */
  void writeTo(OutputStream out, HttpServletResponse servletResponse) {
    Thread.ofVirtual().name("sse-drain").start(this::drain);
    try {
      for (String line = lines.take(); line != END; line = lines.take()) {
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        if (line.isEmpty()) servletResponse.flushBuffer();
      }
      servletResponse.flushBuffer();
    } catch (IOException e) {
      clientGone.set(true);
    } catch (InterruptedException e) {
      clientGone.set(true);
      Thread.currentThread().interrupt();
    } finally {
      lines.clear();
      writerDone.countDown();
    }
  }

  private void drain() {
    Thread self = Thread.currentThread();
    // Closing the stream alone does not wake a reader blocked inside it; the interrupt does.
    ScheduledFuture<?> deadline = watchdog.schedule(() -> {
      deadlineLock.lock();
      try {
        if (drained) return;
        cut.set(true);
        self.interrupt();
      } finally {
        deadlineLock.unlock();
      }
      try {
        upstream.close();
      } catch (IOException ignored) {
        // Already broken; the reader sees that either way.
      }
    }, maxDuration.toMillis(), TimeUnit.MILLISECONDS);

    int in = 0;
    int out = 0;
    boolean reported = false;
    int status = 200;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(upstream, StandardCharsets.UTF_8))) {
      String line;
      boolean droppedEvent = false;
      while ((line = reader.readLine()) != null) {
        if (droppedEvent && line.isEmpty()) {
          droppedEvent = false;
          continue;
        }
        boolean pass = true;
        boolean usageChunk = false;
        if (line.startsWith("data: ") && line.contains("\"usage\"")) {
          try {
            JsonNode chunk = json.readTree(line.substring(6));
            JsonNode u = chunk.path("usage");
            if (u.isObject()) {
              in = u.path("prompt_tokens").asInt(0);
              out = u.path("completion_tokens").asInt(0);
              reported = true;
              usageChunk = chunk.path("choices").isEmpty();
              if (!clientWantsUsage && usageChunk) {
                pass = false;
                droppedEvent = true;
              }
            }
          } catch (JacksonException ignored) {
            // Not a usage chunk after all; relay it untouched.
          }
        }
        if (!usageChunk && line.startsWith("data: {")) keep(line);
        if (pass) forward(line);
      }
    } catch (IOException e) {
      status = cut.get() ? 504 : 502;
    } finally {
      deadline.cancel(false);
      // Once drained is set under the lock the watchdog can no longer interrupt, so clearing the
      // flag after it keeps a late deadline away from the queue offer and the billing call below.
      deadlineLock.lock();
      try {
        drained = true;
      } finally {
        deadlineLock.unlock();
      }
      Thread.interrupted();
      forward(END);
      awaitWriter();
      if (clientGone.get() && status == 200) status = 499;
      double factor = bufferedChars == 0 ? 1 : (double) (bufferedChars + droppedChars) / bufferedChars;
      try {
        billing.bill(new StreamResult(status, reported, in, out, this::outputText, factor));
      } finally {
        onDrained.run();
      }
    }
  }

  private void keep(String line) {
    if (bufferedChars + line.length() > MAX_BUFFERED_CHARS) {
      droppedChars += line.length();
      return;
    }
    payloads.add(line);
    bufferedChars += line.length();
  }

  /** The generated text the stream carried: content, tool-call arguments and reasoning. */
  private String outputText() {
    StringBuilder text = new StringBuilder();
    for (String line : payloads) {
      try {
        for (JsonNode choice : json.readTree(line.substring(6)).path("choices")) {
          JsonNode delta = choice.path("delta");
          if (delta.path("content").isString()) text.append(delta.path("content").asString());
          if (delta.path("reasoning_content").isString()) text.append(delta.path("reasoning_content").asString());
          for (JsonNode call : delta.path("tool_calls")) {
            text.append(call.path("function").path("arguments").asString(""));
          }
        }
      } catch (JacksonException ignored) {
        // A malformed chunk carried no countable text.
      }
    }
    return text.toString();
  }

  /** Blocks while the client is slower than upstream; discards once the client has left. */
  private void forward(String line) {
    try {
      while (!clientGone.get()) {
        if (lines.offer(line, 100, TimeUnit.MILLISECONDS)) return;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void awaitWriter() {
    try {
      writerDone.await(WRITER_GRACE_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
