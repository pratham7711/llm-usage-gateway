package io.github.pratham7711.llmgw.gateway;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/** Deterministic upstream for tests: 11 prompt tokens and 7 completion tokens on every success. */
final class FakeUpstream {

  static final int PROMPT_TOKENS = 11;
  static final int COMPLETION_TOKENS = 7;

  private final HttpServer server;
  final AtomicReference<String> lastBody = new AtomicReference<>();
  final AtomicReference<String> lastAuth = new AtomicReference<>();

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
    String usage = "{\"prompt_tokens\":" + PROMPT_TOKENS + ",\"completion_tokens\":" + COMPLETION_TOKENS
        + ",\"total_tokens\":" + (PROMPT_TOKENS + COMPLETION_TOKENS) + "}";

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
      boolean slow = body.contains("\"slow-stream");
      String[] pieces = slow ? new String[] {"a ", "b ", "c ", "d ", "e ", "f ", "g ", "h "} : new String[] {"fake ", "streamed ", "reply"};
      for (String piece : pieces) {
        out.write(("data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\""
            + piece + "\"}}]}\n\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
        if (slow) pause(250);
      }
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
