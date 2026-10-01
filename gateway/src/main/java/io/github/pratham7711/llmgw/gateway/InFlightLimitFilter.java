package io.github.pratham7711.llmgw.gateway;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Bounds the number of proxied requests in flight and sheds the excess with an immediate 503.
 *
 * Without it, an arrival rate above capacity queues work without limit: every waiting request
 * holds its body, its parsed JSON and an upstream connection, the heap fills, GC takes over and
 * latency collapses for everyone (measured: see docs/DESIGN.md). With it, admitted requests keep
 * their latency and the rest get a cheap, retryable rejection. A streamed reply holds its permit
 * until its upstream stream is fully drained, even after the client has left, because that is
 * when its upstream connection is released.
 */
@Component
public class InFlightLimitFilter extends OncePerRequestFilter {

  /** Request attribute holding the request's {@link Permit}, for work that outlives the response. */
  public static final String PERMIT = InFlightLimitFilter.class.getName() + ".permit";

  /** One in-flight slot, returned to the pool when its last holder releases it. */
  public static final class Permit {
    private final Semaphore pool;
    private final AtomicInteger holders = new AtomicInteger(1);

    private Permit(Semaphore pool) {
      this.pool = pool;
    }

    public Permit retain() {
      holders.incrementAndGet();
      return this;
    }

    public void release() {
      if (holders.decrementAndGet() == 0) pool.release();
    }
  }

  private static final byte[] BODY =
      "{\"error\":{\"type\":\"overloaded\",\"message\":\"Gateway at capacity, retry shortly\"}}".getBytes();

  /**
   * Default permits per available CPU. The limit caps throughput at limit / mean latency (Little's
   * law): a fixed 256 held a 4-CPU gateway to about 5,800 req/s at 44 ms with half its CPU idle,
   * while 1 CPU saturates near 2,900 req/s, which 128 permits already allow.
   */
  static final int PERMITS_PER_CPU = 128;

  private final Semaphore permits;
  private final Counter shed;
  private final int limit;

  public InFlightLimitFilter(@Value("${gateway.max-in-flight:0}") int maxInFlight, MeterRegistry meters) {
    this.limit = maxInFlight > 0 ? maxInFlight : PERMITS_PER_CPU * Runtime.getRuntime().availableProcessors();
    this.permits = new Semaphore(limit);
    this.shed = meters.counter("gateway.requests", "outcome", "shed");
    Gauge.builder("gateway.inflight", permits, p -> limit - p.availablePermits()).register(meters);
    Gauge.builder("gateway.inflight.limit", () -> limit).register(meters);
  }

  int limit() {
    return limit;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    return !request.getRequestURI().startsWith("/v1/");
  }

  @Override
  protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    if (!permits.tryAcquire()) {
      shed.increment();
      response.setStatus(503);
      response.setHeader("Retry-After", "1");
      response.setContentType("application/json");
      response.getOutputStream().write(BODY);
      return;
    }
    Permit permit = new Permit(permits);
    request.setAttribute(PERMIT, permit);
    AtomicBoolean released = new AtomicBoolean();
    Runnable release = () -> {
      if (released.compareAndSet(false, true)) permit.release();
    };
    boolean handedOff = false;
    try {
      chain.doFilter(request, response);
      if (request.isAsyncStarted()) {
        request.getAsyncContext().addListener(new AsyncListener() {
          @Override public void onComplete(AsyncEvent e) { release.run(); }
          @Override public void onTimeout(AsyncEvent e) { release.run(); }
          @Override public void onError(AsyncEvent e) { release.run(); }
          @Override public void onStartAsync(AsyncEvent e) { }
        });
        handedOff = true;
      }
    } finally {
      if (!handedOff) release.run();
    }
  }
}
