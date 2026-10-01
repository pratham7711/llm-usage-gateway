package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.UsageEvent;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

@RestController
public class ChatProxyController {

  private final TenantDirectory tenants;
  private final Admission admission;
  private final UpstreamClient upstream;
  private final UsagePublisher usage;
  private final JsonMapper json;
  private final Clock clock;
  private final MeterRegistry meters;

  public ChatProxyController(TenantDirectory tenants, Admission admission, UpstreamClient upstream,
      UsagePublisher usage, JsonMapper json, Clock clock, MeterRegistry meters) {
    this.tenants = tenants;
    this.admission = admission;
    this.upstream = upstream;
    this.usage = usage;
    this.json = json;
    this.clock = clock;
    this.meters = meters;
  }

  /**
   * Declared as Object on purpose: Spring MVC picks the return-value handler from the runtime type,
   * so a streamed reply (a StreamingResponseBody) and a buffered one (a ResponseEntity) can share
   * one endpoint.
   */
  @PostMapping(path = "/v1/chat/completions")
  public Object chat(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth,
      @RequestBody byte[] body, HttpServletRequest servletRequest, HttpServletResponse servletResponse)
      throws InterruptedException {
    long started = System.nanoTime();

    Optional<Tenant> resolved = tenants.resolve(bearer(auth));
    if (resolved.isEmpty()) return reject(401, "invalid_api_key", "Missing or invalid API key", "unauthorized");
    Tenant tenant = resolved.get();

    ObjectNode req;
    try {
      JsonNode node = json.readTree(body);
      if (!(node instanceof ObjectNode obj)) return reject(400, "invalid_request_error", "Body must be a JSON object", "bad_request");
      req = obj;
    } catch (JacksonException e) {
      return reject(400, "invalid_request_error", "Body is not valid JSON", "bad_request");
    }
    String model = req.path("model").asString("");
    if (model.isBlank()) return reject(400, "invalid_request_error", "model is required", "bad_request");

    // Fail closed: without the admission script there is no rate limit and no quota check, and
    // quota is a billing control. Redis's command timeout bounds how long this can take.
    Admission.Decision decision;
    try {
      decision = admission.check(tenant);
    } catch (DataAccessException e) {
      count("admission_unavailable");
      return ResponseEntity.status(503)
          .header(HttpHeaders.RETRY_AFTER, "1")
          .body(error("admission_unavailable", "Rate limit and quota state is unavailable"));
    }
    switch (decision.outcome()) {
      case RATE_LIMITED -> {
        count("rate_limited");
        long retrySec = Math.max(1, (decision.retryAfterMs() + 999) / 1000);
        return ResponseEntity.status(429)
            .header(HttpHeaders.RETRY_AFTER, String.valueOf(retrySec))
            .body(error("rate_limit_exceeded", "Rate limit exceeded for tenant " + tenant.id()));
      }
      case QUOTA_EXHAUSTED -> {
        return reject(429, "insufficient_quota", "Monthly token quota exhausted for tenant " + tenant.id(), "quota_exhausted");
      }
      case ADMITTED -> { }
    }

    boolean stream = req.path("stream").asBoolean(false);
    if (!stream) return forward(tenant, model, req, started);

    boolean clientWantsUsage = req.path("stream_options").path("include_usage").asBoolean(false);
    JsonNode opts = req.get("stream_options");
    ObjectNode streamOptions = opts instanceof ObjectNode o ? o : req.putObject("stream_options");
    streamOptions.put("include_usage", true);
    return forwardStreaming(tenant, model, req, clientWantsUsage, started, servletRequest, servletResponse);
  }

  private ResponseEntity<?> forward(Tenant tenant, String model, ObjectNode req, long started)
      throws InterruptedException {
    HttpResponse<byte[]> resp;
    try {
      resp = upstream.send(json.writeValueAsBytes(req));
    } catch (IOException e) {
      return upstreamFailure(tenant, model, false, started, e);
    }
    int status = resp.statusCode();
    int in = 0;
    int out = 0;
    if (status / 100 == 2) {
      try {
        JsonNode u = json.readTree(resp.body()).path("usage");
        in = u.path("prompt_tokens").asInt(0);
        out = u.path("completion_tokens").asInt(0);
      } catch (JacksonException ignored) {
        // An unparseable success body is still relayed; it is billed with zero tokens and visible in metrics.
      }
    }
    usage.publish(event(tenant, model, in, out, started, status, false));
    count(status / 100 == 2 ? "ok" : "upstream_error");
    return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(resp.body());
  }

  private Object forwardStreaming(Tenant tenant, String model, ObjectNode req,
      boolean clientWantsUsage, long started, HttpServletRequest servletRequest, HttpServletResponse servletResponse)
      throws InterruptedException {
    HttpResponse<InputStream> resp;
    try {
      resp = upstream.stream(json.writeValueAsBytes(req));
    } catch (IOException e) {
      return upstreamFailure(tenant, model, true, started, e);
    }
    if (resp.statusCode() / 100 != 2) {
      byte[] errBody;
      try (InputStream is = resp.body()) {
        errBody = is.readAllBytes();
      } catch (IOException e) {
        errBody = new byte[0];
      }
      usage.publish(event(tenant, model, 0, 0, started, resp.statusCode(), true));
      count("upstream_error");
      return ResponseEntity.status(resp.statusCode()).contentType(MediaType.APPLICATION_JSON).body(errBody);
    }

    count("ok");
    servletResponse.setStatus(200);
    servletResponse.setContentType(MediaType.TEXT_EVENT_STREAM_VALUE);
    servletResponse.setCharacterEncoding("UTF-8");
    servletResponse.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
    // The upstream drain can outlive a disconnected client, and it holds an upstream connection
    // until it finishes, so it keeps its own hold on the in-flight permit.
    InFlightLimitFilter.Permit permit = (InFlightLimitFilter.Permit) servletRequest.getAttribute(InFlightLimitFilter.PERMIT);
    StreamingResponseBody relay = out -> {
      Runnable drained = permit == null ? () -> { } : permit.retain()::release;
      new SseRelay(resp.body(), clientWantsUsage, json,
          (in, outTokens, status) -> usage.publish(event(tenant, model, in, outTokens, started, status, true)),
          drained).writeTo(out, servletResponse);
    };
    return relay;
  }

  private ResponseEntity<?> upstreamFailure(Tenant tenant, String model, boolean streamed, long started, IOException e) {
    int status = e instanceof HttpTimeoutException ? 504 : 502;
    usage.publish(event(tenant, model, 0, 0, started, status, streamed));
    count("upstream_error");
    String type = e instanceof ConnectException ? "upstream_unreachable" : "upstream_error";
    return ResponseEntity.status(status).body(error(type, "Upstream call failed"));
  }

  private UsageEvent event(Tenant tenant, String model, int in, int out, long started, int status, boolean streamed) {
    long latencyMs = (System.nanoTime() - started) / 1_000_000;
    return new UsageEvent(UUID.randomUUID(), tenant.id(), model, in, out, latencyMs, status, streamed, clock.instant());
  }

  private ResponseEntity<?> reject(int status, String type, String message, String outcome) {
    count(outcome);
    return ResponseEntity.status(status).body(error(type, message));
  }

  private void count(String outcome) {
    meters.counter("gateway.requests", "outcome", outcome).increment();
  }

  private static Map<String, Object> error(String type, String message) {
    return Map.of("error", Map.of("type", type, "message", message));
  }

  private static String bearer(String auth) {
    if (auth == null) return null;
    return auth.regionMatches(true, 0, "Bearer ", 0, 7) ? auth.substring(7).trim() : null;
  }
}
