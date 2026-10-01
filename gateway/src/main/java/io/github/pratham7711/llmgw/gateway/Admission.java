package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.Topics;
import java.time.Clock;
import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/** Rate limit and quota in one Redis round trip (see admission.lua). */
@Component
public class Admission {

  public enum Outcome { ADMITTED, RATE_LIMITED, QUOTA_EXHAUSTED }

  public record Decision(Outcome outcome, long retryAfterMs) {}

  @SuppressWarnings("rawtypes")
  private final DefaultRedisScript<List> script;
  private final StringRedisTemplate redis;
  private final Clock clock;

  public Admission(StringRedisTemplate redis, Clock clock) {
    this.redis = redis;
    this.clock = clock;
    this.script = new DefaultRedisScript<>();
    this.script.setLocation(new ClassPathResource("admission.lua"));
    this.script.setResultType(List.class);
  }

  public Decision check(Tenant tenant) {
    String month = Topics.month(clock.instant());
    List<?> r = redis.execute(script,
        List.of(Topics.bucketKey(tenant.id()), Topics.quotaKey(tenant.id(), month)),
        String.valueOf(tenant.ratePerSec()),
        String.valueOf(tenant.burst()),
        String.valueOf(tenant.monthlyTokenQuota()));
    long code = ((Number) r.get(0)).longValue();
    long retry = ((Number) r.get(1)).longValue();
    if (code == 1) return new Decision(Outcome.ADMITTED, 0);
    if (code == 0) return new Decision(Outcome.RATE_LIMITED, retry);
    return new Decision(Outcome.QUOTA_EXHAUSTED, 0);
  }
}
