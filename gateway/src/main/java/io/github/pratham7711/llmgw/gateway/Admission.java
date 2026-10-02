package io.github.pratham7711.llmgw.gateway;

import io.github.pratham7711.llmgw.common.Topics;
import java.time.Duration;
import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * Quota, rate limit and the request's lease in one Redis round trip (admission.lua), and its
 * settlement in another (settle.lua).
 */
@Component
public class Admission {

  /**
   * QUOTA_RESERVED: what is left of the quota is held by requests still in flight, which usually use
   * less than they reserved, so the request may fit once they settle.
   */
  public enum Outcome { ADMITTED, RATE_LIMITED, QUOTA_EXHAUSTED, QUOTA_RESERVED, TOO_MANY_OPEN_LEASES }

  /** {@code grantedOutput} is the most output tokens the request may produce; set when admitted. */
  public record Decision(Outcome outcome, long retryAfterMs, int grantedOutput) {}

  /** What admission reserved for one request: needed again to settle it. */
  public record Lease(String id, String tenantId, String month, int promptHold, int grantedOutput) {}

  private static final long MONTH_KEY_TTL_SECONDS = Duration.ofDays(40).toSeconds();

  @SuppressWarnings("rawtypes")
  private final DefaultRedisScript<List> admit;
  private final DefaultRedisScript<Long> settle;
  private final StringRedisTemplate redis;
  private final GatewayProperties props;

  public Admission(StringRedisTemplate redis, GatewayProperties props) {
    this.redis = redis;
    this.props = props;
    this.admit = new DefaultRedisScript<>();
    this.admit.setLocation(new ClassPathResource("admission.lua"));
    this.admit.setResultType(List.class);
    this.settle = new DefaultRedisScript<>();
    this.settle.setLocation(new ClassPathResource("settle.lua"));
    this.settle.setResultType(Long.class);
  }

  /**
   * @param leaseTemplate the usage event to bill if the lease expires unsettled, as JSON
   */
  public Decision check(Tenant tenant, String leaseId, String month, int promptHold, int maxOutput, String leaseTemplate) {
    List<?> r = redis.execute(admit,
        List.of(Topics.bucketKey(tenant.id()), Topics.quotaKey(tenant.id(), month), Topics.heldKey(tenant.id(), month),
            Topics.leaseKey(tenant.id()), Topics.leaseExpiryKey(tenant.id())),
        String.valueOf(tenant.ratePerSec()),
        String.valueOf(tenant.burst()),
        String.valueOf(tenant.monthlyTokenQuota()),
        leaseId,
        String.valueOf(promptHold),
        String.valueOf(maxOutput),
        String.valueOf(props.leaseTtl().toMillis()),
        month,
        leaseTemplate,
        String.valueOf(props.maxOpenLeasesPerTenant()),
        String.valueOf(MONTH_KEY_TTL_SECONDS));
    long code = ((Number) r.get(0)).longValue();
    long retry = ((Number) r.get(1)).longValue();
    int grant = (int) ((Number) r.get(2)).longValue();
    if (code == 1) return new Decision(Outcome.ADMITTED, 0, grant);
    if (code == 0) return new Decision(Outcome.RATE_LIMITED, retry, 0);
    if (code == -2) return new Decision(Outcome.TOO_MANY_OPEN_LEASES, 0, 0);
    if (code == -3) return new Decision(Outcome.QUOTA_RESERVED, 0, 0);
    return new Decision(Outcome.QUOTA_EXHAUSTED, 0, 0);
  }

  /**
   * Releases the lease's reservation and counts the billed tokens in the month the event is billed
   * to. Returns false if the lease had already expired, in which case the reaper owns it.
   */
  public boolean settle(Lease lease, String billedMonth, long billedTokens, String eventJson) {
    Long r = redis.execute(settle,
        List.of(Topics.leaseKey(lease.tenantId()), Topics.heldKey(lease.tenantId(), lease.month()),
            Topics.quotaKey(lease.tenantId(), billedMonth)),
        lease.id(), String.valueOf(billedTokens), eventJson, String.valueOf(MONTH_KEY_TTL_SECONDS));
    return r != null && r == 1;
  }
}
