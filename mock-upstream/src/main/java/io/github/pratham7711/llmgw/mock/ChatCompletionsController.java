package io.github.pratham7711.llmgw.mock;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import tools.jackson.databind.json.JsonMapper;

/** A fake OpenAI-compatible chat endpoint. It spends no money and its latency is known in advance. */
@RestController
public class ChatCompletionsController {

  private final MockProperties props;
  private final JsonMapper json;
  private final RequestLog requests;

  public ChatCompletionsController(MockProperties props, JsonMapper json, RequestLog requests) {
    this.props = props;
    this.json = json;
    this.requests = requests;
  }

  /** Returns Object so Spring MVC picks the handler from the runtime type (stream or buffered reply). */
  @PostMapping(path = "/v1/chat/completions")
  public Object complete(@RequestBody Map<String, Object> body, HttpServletResponse servletResponse,
      @RequestHeader(value = "X-Request-Id", required = false) String requestId) {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    String model = String.valueOf(body.getOrDefault("model", "mock-small"));
    int promptTokens = estimatePromptTokens(body.get("messages"));
    int maxTokens = body.get("max_completion_tokens") instanceof Number c ? c.intValue()
        : body.get("max_tokens") instanceof Number n ? n.intValue() : 256;
    int completionTokens = Math.max(1, Math.min(maxTokens, 16 + rnd.nextInt(241)));
    long latency = sampleLatencyMs(rnd);

    if (props.errorRate() > 0 && rnd.nextDouble() < props.errorRate()) {
      sleep(latency / 4);
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(Map.of("error", Map.of("type", "upstream_overloaded", "message", "mock failure")));
    }

    // A real provider bills a request once it starts generating, whether or not the reply arrives.
    requests.served(requestId, promptTokens, completionTokens);
    String id = "chatcmpl-" + UUID.randomUUID();
    Map<String, Object> usage = usage(promptTokens, completionTokens);
    String text = "mock ".repeat(Math.min(completionTokens, 64)).trim();

    if (Boolean.TRUE.equals(body.get("stream"))) {
      boolean includeUsage =
          body.get("stream_options") instanceof Map<?, ?> so && Boolean.TRUE.equals(so.get("include_usage"));
      servletResponse.setStatus(200);
      servletResponse.setContentType(MediaType.TEXT_EVENT_STREAM_VALUE);
      servletResponse.setCharacterEncoding("UTF-8");
      StreamingResponseBody stream = out -> streamChunks(out, servletResponse, id, model, text, usage, includeUsage, latency);
      return stream;
    }

    sleep(latency);
    Map<String, Object> resp = new LinkedHashMap<>();
    resp.put("id", id);
    resp.put("object", "chat.completion");
    resp.put("created", System.currentTimeMillis() / 1000);
    resp.put("model", model);
    resp.put("choices", List.of(Map.of(
        "index", 0,
        "message", Map.of("role", "assistant", "content", text),
        "finish_reason", "stop")));
    resp.put("usage", usage);
    return ResponseEntity.ok(resp);
  }

  private void streamChunks(OutputStream out, HttpServletResponse res, String id, String model, String text,
      Map<String, Object> usage, boolean includeUsage, long latency) throws IOException {
    int chunks = props.streamChunks();
    String[] words = text.split(" ");
    long perChunk = Math.max(1, latency / chunks);
    for (int i = 0; i < chunks; i++) {
      sleep(perChunk);
      int from = i * words.length / chunks;
      int to = (i + 1) * words.length / chunks;
      String piece = String.join(" ", java.util.Arrays.copyOfRange(words, from, to)) + " ";
      Map<String, Object> chunk = new LinkedHashMap<>();
      chunk.put("id", id);
      chunk.put("object", "chat.completion.chunk");
      chunk.put("model", model);
      chunk.put("choices", List.of(Map.of(
          "index", 0,
          "delta", Map.of("content", piece),
          "finish_reason", i == chunks - 1 ? "stop" : "")));
      write(out, res, json.writeValueAsString(chunk));
    }
    if (includeUsage) {
      Map<String, Object> last = new LinkedHashMap<>();
      last.put("id", id);
      last.put("object", "chat.completion.chunk");
      last.put("model", model);
      last.put("choices", List.of());
      last.put("usage", usage);
      write(out, res, json.writeValueAsString(last));
    }
    write(out, res, "[DONE]");
  }

  /** out.flush() is a no-op here (Spring wraps it in a NonFlushingOutputStream); flush the response. */
  private static void write(OutputStream out, HttpServletResponse res, String data) throws IOException {
    out.write(("data: " + data + "\n\n").getBytes(StandardCharsets.UTF_8));
    res.flushBuffer();
  }

  private long sampleLatencyMs(ThreadLocalRandom rnd) {
    double sample = props.medianMs() * Math.exp(props.sigma() * rnd.nextGaussian());
    return Math.min(props.maxMs(), Math.max(1, Math.round(sample)));
  }

  static int estimatePromptTokens(Object messages) {
    int chars = 0;
    if (messages instanceof List<?> list) {
      for (Object m : list) {
        if (m instanceof Map<?, ?> map && map.get("content") instanceof String s) chars += s.length();
      }
    }
    return Math.max(1, (chars + 3) / 4);
  }

  private static Map<String, Object> usage(int prompt, int completion) {
    return Map.of("prompt_tokens", prompt, "completion_tokens", completion, "total_tokens", prompt + completion);
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
