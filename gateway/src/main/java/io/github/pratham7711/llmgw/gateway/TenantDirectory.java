package io.github.pratham7711.llmgw.gateway;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Resolves an API key to its tenant. Keys are looked up by SHA-256 digest, so the database never
 * holds a usable key. Hits are cached for a minute; misses for ten seconds, so a client retrying a
 * bad key cannot turn every request into a database query.
 */
@Component
public class TenantDirectory {

  private static final String LOOKUP = """
      select t.id, t.rate_per_sec, t.burst, t.monthly_token_quota
      from api_key k join tenant t on t.id = k.tenant_id
      where k.key_hash = ? and k.revoked_at is null
      """;

  private final JdbcClient jdbc;
  private final Cache<String, Tenant> hits =
      Caffeine.newBuilder().maximumSize(100_000).expireAfterWrite(Duration.ofSeconds(60)).build();
  private final Cache<String, Boolean> misses =
      Caffeine.newBuilder().maximumSize(10_000).expireAfterWrite(Duration.ofSeconds(10)).build();

  public TenantDirectory(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  public Optional<Tenant> resolve(String apiKey) {
    if (apiKey == null || apiKey.isBlank()) return Optional.empty();
    String hash = sha256Hex(apiKey);
    Tenant cached = hits.getIfPresent(hash);
    if (cached != null) return Optional.of(cached);
    if (misses.getIfPresent(hash) != null) return Optional.empty();

    Optional<Tenant> found = jdbc.sql(LOOKUP).param(hash)
        .query((rs, n) -> new Tenant(rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getLong(4)))
        .optional();
    found.ifPresentOrElse(t -> hits.put(hash, t), () -> misses.put(hash, Boolean.TRUE));
    return found;
  }

  public void invalidateAll() {
    hits.invalidateAll();
    misses.invalidateAll();
  }

  static String sha256Hex(String s) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(md.digest(s.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
