package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.Topics;
import io.github.pratham7711.llmgw.common.UsageEvent;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger log = LoggerFactory.getLogger(ChatProxyController.class);
  /** Response header naming the output limit sent upstream when it is lower than the client asked for. */
  static final String GRANTED_MAX_TOKENS = "X-Granted-Max-Tokens";

  private final TenantDirectory tenants;
  private final Admission admission;
  private final UpstreamClient upstream;
  private final UsagePublisher usage;
  private final TokenCounter tokens;
  private final PromptCalibration calibration;
  private final GatewayProperties props;
  private final ScheduledExecutorService streamDeadlines;
  private final JsonMapper json;
  private final Clock clock;
  private final MeterRegistry meters;
  private final DistributionSummary promptEstimateError;
  private final DistributionSummary outputEstimateError;

  public ChatProxyController(TenantDirectory tenants, Admission admission, UpstreamClient upstream,
      UsagePublisher usage, TokenCounter tokens, PromptCalibration calibration, GatewayProperties props,
      ScheduledExecutorService streamDeadlines, JsonMapper json, Clock clock, MeterRegistry meters) {
    this.tenants = tenants;
    this.admission = admission;
    this.upstream = upstream;
    this.usage = usage;
    this.tokens = tokens;
    this.calibration = calibration;
    this.props = props;
    this.streamDeadlines = streamDeadlines;
    this.json = json;
    this.clock = clock;
    this.meters = meters;
    this.promptEstimateError = DistributionSummary.builder("gateway.token.estimate.error")
        .description("Gateway count minus provider count, in percent of the provider count")
        .baseUnit("percent").tag("part", "prompt").publishPercentiles(0.5, 0.9, 0.99).register(meters);
    this.outputEstimateError = DistributionSummary.builder("gateway.token.estimate.error")
        .description("Gateway count minus provider count, in percent of the provider count")
        .baseUnit("percent").tag("part", "output").publishPercentiles(0.5, 0.9, 0.99).register(meters);
  }

  /** Everything needed to settle and bill one admitted request. */
  private record Call(Admission.Lease lease, Tenant tenant, String model, boolean streamed, long startedNanos,
      int promptEstimate) {}

  /** Billed token counts for one call and where they came from. */
  private record Billed(int in, int out, String source, Integer estimatedOut) {}

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
    boolean stream = req.path("stream").asBoolean(false);

    // Price the request before it is forwarded: the prompt by counting it, the output by the most
    // the request may generate. The lease reserves both until the real usage is known.
    Instant admittedAt = clock.instant();
    String month = Topics.month(admittedAt);
    UUID leaseId = UUID.randomUUID();
    int promptEstimate = tokens.prompt(model, req);
    int promptHold = calibration.hold(model, promptEstimate, props.promptEstimateMargin());
    OutputLimit limit = OutputLimit.of(req, model);
    int maxOutput = limit.requested() > 0 ? limit.requested() : props.defaultMaxOutputTokens();
    String template = json.writeValueAsString(new UsageEvent(leaseId, tenant.id(), model, promptEstimate, 0, 0, 0,
        stream, admittedAt, UsageEvent.SOURCE_LEASE_EXPIRED, promptEstimate, null));

    // Fail closed: without the admission script there is no rate limit and no quota check, and
    // quota is a billing control. Redis's command timeout bounds how long this can take.
    Admission.Decision decision;
    try {
      decision = admission.check(tenant, leaseId.toString(), month, promptHold, maxOutput, template);
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
      case QUOTA_RESERVED -> {
        count("quota_reserved");
        return ResponseEntity.status(429)
            .header(HttpHeaders.RETRY_AFTER, "1")
            .body(error("quota_reserved", "The rest of the monthly quota is reserved by requests in flight; retry shortly"));
      }
      case TOO_MANY_OPEN_LEASES -> {
        count("billing_backlog");
        return ResponseEntity.status(503)
            .header(HttpHeaders.RETRY_AFTER, "5")
            .body(error("billing_backlog", "Usage for this tenant cannot be recorded right now"));
      }
      case ADMITTED -> { }
    }

    int granted = decision.grantedOutput();
    limit.apply(req, granted);
    if (granted < maxOutput) servletResponse.setHeader(GRANTED_MAX_TOKENS, String.valueOf(granted));
    Call call = new Call(new Admission.Lease(leaseId.toString(), tenant.id(), month, promptHold, granted),
        tenant, model, stream, started, promptEstimate);

    if (!stream) return forward(call, req);

    boolean clientWantsUsage = req.path("stream_options").path("include_usage").asBoolean(false);
    JsonNode opts = req.get("stream_options");
    ObjectNode streamOptions = opts instanceof ObjectNode o ? o : req.putObject("stream_options");
    streamOptions.put("include_usage", true);
    return forwardStreaming(call, req, clientWantsUsage, servletRequest, servletResponse);
  }

  private ResponseEntity<?> forward(Call call, ObjectNode req) throws InterruptedException {
    HttpResponse<byte[]> resp;
    try {
      resp = upstream.send(json.writeValueAsBytes(req), call.lease().id());
    } catch (IOException e) {
      return upstreamFailure(call, e);
    }
    int status = resp.statusCode();
    Billed billed = new Billed(0, 0, UsageEvent.SOURCE_NONE, null);
    if (status / 100 == 2) {
      JsonNode root = parseOrNull(resp.body());
      // An unparseable body is relayed as is. The provider processed the prompt, so that is billed.
      billed = root == null
          ? new Billed(call.promptEstimate(), 0, UsageEvent.SOURCE_ESTIMATED, 0)
          : billedFrom(call, root.path("usage"), () -> completionText(root), 1.0);
    }
    complete(call, status, billed);
    count(status / 100 == 2 ? "ok" : "upstream_error");
    return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(resp.body());
  }

  private Object forwardStreaming(Call call, ObjectNode req, boolean clientWantsUsage,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws InterruptedException {
    HttpResponse<InputStream> resp;
    try {
      resp = upstream.stream(json.writeValueAsBytes(req), call.lease().id());
    } catch (IOException e) {
      return upstreamFailure(call, e);
    }
    if (resp.statusCode() / 100 != 2) {
      byte[] errBody;
      try (InputStream is = resp.body()) {
        errBody = is.readAllBytes();
      } catch (IOException e) {
        errBody = new byte[0];
      }
      complete(call, resp.statusCode(), new Billed(0, 0, UsageEvent.SOURCE_NONE, null));
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
          r -> complete(call, r.status(), streamBilled(call, r)),
          drained, streamDeadlines, props.maxStreamDuration()).writeTo(out, servletResponse);
    };
    return relay;
  }

  private Billed streamBilled(Call call, SseRelay.StreamResult r) {
    if (r.usageReported()) {
      ObjectNode usage = json.createObjectNode().put("prompt_tokens", r.promptTokens()).put("completion_tokens", r.completionTokens());
      return billedFrom(call, usage, r.outputText(), r.unbufferedFactor());
    }
    return billedFrom(call, json.createObjectNode(), r.outputText(), r.unbufferedFactor());
  }

  private JsonNode parseOrNull(byte[] body) {
    try {
      return json.readTree(body);
    } catch (JacksonException e) {
      return null;
    }
  }

  /**
   * The provider's usage report when there is one. Without it the provider has still processed the
   * prompt and generated what came back, so both are counted locally and marked as estimated.
   */
  private Billed billedFrom(Call call, JsonNode usage, java.util.function.Supplier<String> output, double factor) {
    if (usage.isObject() && usage.has("prompt_tokens")) {
      int in = usage.path("prompt_tokens").asInt(0);
      int out = usage.path("completion_tokens").asInt(0);
      // The prompt was counted at admission anyway, so its drift is tracked on every response;
      // counting the output costs a tokenizer pass, so that is sampled.
      calibration.observe(call.model(), call.promptEstimate(), in, props.promptEstimateMargin());
      record(promptEstimateError, call.promptEstimate(), in);
      Integer estimatedOut = null;
      if (ThreadLocalRandom.current().nextDouble() < props.estimateSampleRate()) {
        estimatedOut = (int) Math.round(tokens.text(call.model(), output.get()) * factor);
        record(outputEstimateError, estimatedOut, out);
      }
      return new Billed(in, out, UsageEvent.SOURCE_PROVIDER, estimatedOut);
    }
    int out = (int) Math.round(tokens.text(call.model(), output.get()) * factor);
    return new Billed(call.promptEstimate(), out, UsageEvent.SOURCE_ESTIMATED, out);
  }

  private static void record(DistributionSummary error, int estimated, int reported) {
    if (reported > 0) error.record(100.0 * (estimated - reported) / reported);
  }

  private static String completionText(JsonNode root) {
    StringBuilder text = new StringBuilder();
    for (JsonNode choice : root.path("choices")) {
      JsonNode msg = choice.path("message");
      if (msg.path("content").isString()) text.append(msg.path("content").asString());
      if (msg.path("reasoning_content").isString()) text.append(msg.path("reasoning_content").asString());
      for (JsonNode c : msg.path("tool_calls")) text.append(c.path("function").path("arguments").asString(""));
    }
    return text.toString();
  }

  private ResponseEntity<?> upstreamFailure(Call call, IOException e) {
    int status = e instanceof HttpTimeoutException ? 504 : 502;
    complete(call, status, new Billed(0, 0, UsageEvent.SOURCE_NONE, null));
    count("upstream_error");
    String type = e instanceof ConnectException ? "upstream_unreachable" : "upstream_error";
    return ResponseEntity.status(status).body(error(type, "Upstream call failed"));
  }

  /**
   * Settles the lease, then publishes the event. Settling first means the quota already counts the
   * request while its event is on its way to Postgres; the lease keeps the event until Kafka has it.
   */
  private void complete(Call call, int status, Billed billed) {
    Instant at = clock.instant();
    long latencyMs = (System.nanoTime() - call.startedNanos()) / 1_000_000;
    UsageEvent event = new UsageEvent(UUID.fromString(call.lease().id()), call.tenant().id(), call.model(),
        billed.in(), billed.out(), latencyMs, status, call.streamed(), at, billed.source(), call.promptEstimate(),
        billed.estimatedOut());
    meters.counter("gateway.usage.source", "source", billed.source()).increment();
    boolean settled = true;
    try {
      if (!admission.settle(call.lease(), Topics.month(at), event.totalTokens(), json.writeValueAsString(event))) {
        // Only possible for a request that outlived its lease; the reaper has billed the reservation.
        meters.counter("gateway.leases", "result", "expired_before_settle").increment();
      }
    } catch (DataAccessException e) {
      // The reservation is still held. Billing does not depend on Redis: the event is published
      // anyway, and the lease is left for the reaper, which turns the reservation into usage.
      settled = false;
      meters.counter("gateway.leases", "result", "settle_failed").increment();
      log.warn("settle failed for lease {}: {}", call.lease().id(), e.toString());
    }
    usage.publish(event, settled);
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

  /**
   * The output limit a request asked for, and the field it used. Reasoning models take only
   * max_completion_tokens; older ones take max_tokens.
   */
  record OutputLimit(String field, int requested) {

    static OutputLimit of(JsonNode req, String model) {
      if (req.path("max_completion_tokens").isNumber()) {
        return new OutputLimit("max_completion_tokens", req.path("max_completion_tokens").asInt());
      }
      if (req.path("max_tokens").isNumber()) return new OutputLimit("max_tokens", req.path("max_tokens").asInt());
      String m = model.toLowerCase();
      boolean reasoning = m.startsWith("o1") || m.startsWith("o3") || m.startsWith("o4") || m.startsWith("gpt-5");
      return new OutputLimit(reasoning ? "max_completion_tokens" : "max_tokens", -1);
    }

    /** Sends the granted limit upstream whenever it is tighter than the request's own. */
    void apply(ObjectNode req, int granted) {
      if (requested <= 0 || granted < requested) req.put(field, granted);
    }
  }
}
