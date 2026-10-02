package io.github.pratham7711.llmgw.mock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * What the mock provider served while recording is on: the provider's side of the ledger, for
 * reconciling against what the gateway billed. Requests are kept by the gateway's X-Request-Id
 * when it sends one, and always counted. Off by default, since it grows by one entry per request.
 */
@Component
public class RequestLog {

  public record Served(int promptTokens, int completionTokens) {}

  private final Map<String, Served> served = new ConcurrentHashMap<>();
  private final AtomicLong requests = new AtomicLong();
  private final AtomicLong tokens = new AtomicLong();
  private volatile boolean recording;

  void served(String requestId, int promptTokens, int completionTokens) {
    if (!recording) return;
    requests.incrementAndGet();
    tokens.addAndGet(promptTokens + completionTokens);
    if (requestId != null) served.put(requestId, new Served(promptTokens, completionTokens));
  }

  void reset(boolean record) {
    recording = false;
    served.clear();
    requests.set(0);
    tokens.set(0);
    recording = record;
  }

  long requests() {
    return requests.get();
  }

  long tokens() {
    return tokens.get();
  }

  Map<String, Served> snapshot() {
    return Map.copyOf(served);
  }
}
