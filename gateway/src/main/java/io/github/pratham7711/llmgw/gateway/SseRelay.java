package io.github.pratham7711.llmgw.gateway;

import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * Lines pass through a small bounded queue, so a slow client still applies backpressure upstream.
 */
final class SseRelay {

  /** Receives the usage once the upstream stream has been fully read. */
  interface Billing {
    void bill(int tokensIn, int tokensOut, int status);
  }

  private static final String END = new String("end-of-stream");
  private static final long WRITER_GRACE_SECONDS = 30;

  private final BlockingQueue<String> lines = new ArrayBlockingQueue<>(256);
  private final AtomicBoolean clientGone = new AtomicBoolean();
  private final CountDownLatch writerDone = new CountDownLatch(1);
  private final InputStream upstream;
  private final boolean clientWantsUsage;
  private final JsonMapper json;
  private final Billing billing;
  private final Runnable onDrained;

  SseRelay(InputStream upstream, boolean clientWantsUsage, JsonMapper json, Billing billing, Runnable onDrained) {
    this.upstream = upstream;
    this.clientWantsUsage = clientWantsUsage;
    this.json = json;
    this.billing = billing;
    this.onDrained = onDrained;
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
    int in = 0;
    int out = 0;
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
        if (line.startsWith("data: ") && line.contains("\"usage\"")) {
          try {
            JsonNode chunk = json.readTree(line.substring(6));
            JsonNode u = chunk.path("usage");
            if (u.isObject()) {
              in = u.path("prompt_tokens").asInt(0);
              out = u.path("completion_tokens").asInt(0);
              if (!clientWantsUsage && chunk.path("choices").isEmpty()) {
                pass = false;
                droppedEvent = true;
              }
            }
          } catch (JacksonException ignored) {
            // Not a usage chunk after all; relay it untouched.
          }
        }
        if (pass) forward(line);
      }
    } catch (IOException e) {
      status = 502;
    } finally {
      forward(END);
      awaitWriter();
      if (clientGone.get() && status == 200) status = 499;
      try {
        billing.bill(in, out, status);
      } finally {
        onDrained.run();
      }
    }
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
