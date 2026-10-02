package io.github.pratham7711.llmgw.gateway;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deterministic upstream for tests: 11 prompt tokens and 7 completion tokens on every success.
 *
 * Models whose name contains these markers behave differently:
 * "fail-503" answers 503; "slow-stream" spaces its chunks 250 ms apart; "fill-max" generates
 * exactly max_tokens (the worst case a quota has to survive) and reports the prompt as the gateway
 * counts it; "cut-stream" ends the stream after its content with no usage chunk; "endless-stream"
 * streams until the gateway hangs up.
 */
final class FakeUpstream {

  static final int PROMPT_TOKENS = 11;
  static final int COMPLETION_TOKENS = 7;

  private final HttpServer server;
  final AtomicReference<String> lastBody = new AtomicReference<>();
  final AtomicReference<String> lastAuth = new AtomicReference<>();
  final AtomicReference<Integer> lastMaxTokens = new AtomicReference<>();
  final Set<String> requestIds = ConcurrentHashMap.newKeySet();
  private static final Pattern MAX_TOKENS = Pattern.compile("\"max_tokens\":(\\d+)");
  /** What the gateway's tokenizer counts for one user message "hi": 3 + 3 framing, "user", "hi". */
  static final int HI_PROMPT_TOKENS = 8;

  FakeUpstream() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.createContext("/v1/chat/completions", this::handle);
    server.start();
  }

  String baseUrl() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private void handle(HttpExchange ex) throws IOException {
    String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    lastBody.set(body);
    lastAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
    String requestId = ex.getRequestHeaders().getFirst("X-Request-Id");
    if (requestId != null) requestIds.add(requestId);
    Matcher m = MAX_TOKENS.matcher(body);
    Integer maxTokens = m.find() ? Integer.valueOf(m.group(1)) : null;
    lastMaxTokens.set(maxTokens);
    int prompt = PROMPT_TOKENS;
    int completion = COMPLETION_TOKENS;
    if (body.contains("fill-max") && maxTokens != null) {
      prompt = HI_PROMPT_TOKENS;
      completion = maxTokens;
    }
    String usage = "{\"prompt_tokens\":" + prompt + ",\"completion_tokens\":" + completion
        + ",\"total_tokens\":" + (prompt + completion) + "}";

    if (body.contains("\"fail-503\"")) {
      send(ex, 503, "application/json", "{\"error\":{\"type\":\"overloaded\"}}");
      return;
    }
    if (!body.contains("\"stream\":true")) {
      send(ex, 200, "application/json",
          "{\"id\":\"c1\",\"object\":\"chat.completion\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\","
              + "\"content\":\"fake reply\"},\"finish_reason\":\"stop\"}],\"usage\":" + usage + "}");
      return;
    }
    ex.getResponseHeaders().set("Content-Type", "text/event-stream");
    ex.sendResponseHeaders(200, 0);
    try (OutputStream out = ex.getResponseBody()) {
      if (body.contains("endless-stream")) {
        // Ends when a write fails, i.e. when the gateway has cut the stream.
        while (true) {
          out.write("data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"more \"}}]}\n\n"
              .getBytes(StandardCharsets.UTF_8));
          out.flush();
          pause(100);
        }
      }
      boolean slow = body.contains("\"slow-stream");
      String[] pieces = slow ? new String[] {"a ", "b ", "c ", "d ", "e ", "f ", "g ", "h "} : new String[] {"fake ", "streamed ", "reply"};
      for (String piece : pieces) {
        out.write(("data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\""
            + piece + "\"}}]}\n\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
        if (slow) pause(250);
      }
      if (body.contains("cut-stream")) return;
      if (body.contains("\"include_usage\":true")) {
        out.write(("data: {\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":" + usage + "}\n\n")
            .getBytes(StandardCharsets.UTF_8));
      }
      out.write("data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void send(HttpExchange ex, int status, String type, String body) throws IOException {
    byte[] b = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", type);
    ex.sendResponseHeaders(status, b.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(b);
    }
  }

  private static void pause(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  void stop() {
    server.stop(0);
  }
}
