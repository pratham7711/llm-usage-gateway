package io.github.pratham7711.llmgw.metering;

import io.github.pratham7711.llmgw.common.Topics;
import io.github.pratham7711.llmgw.common.UsageEvent;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes one batch of usage events in a single transaction. The event insert is
 * {@code ON CONFLICT DO NOTHING ... RETURNING}, and only the rows it actually inserted feed the
 * rollups and monthly totals, so replaying a batch (after a crash before the offset commit, or a
 * producer retry) changes nothing.
 */
@Repository
public class UsageStore {

  public record Located(UsageEvent event, int partition, long offset) {}

  public record MonthTotal(String tenantId, String month, long tokens) {}

  public record Result(int inserted, int duplicates, List<MonthTotal> totals) {}

  private record RollupKey(String tenantId, Instant minute, String model) {}

  private static final class Rollup {
    long requests, errors, tokensIn, tokensOut;
  }

  private record MonthKey(String tenantId, String month) {}

  private static final class Month {
    long tokens, requests;
  }

  private static final String INSERT_EVENTS = """
      insert into usage_event (event_id, tenant_id, model, tokens_in, tokens_out, latency_ms, status,
                               streamed, occurred_at, kafka_partition, kafka_offset)
      select * from unnest(?::uuid[], ?::text[], ?::text[], ?::int[], ?::int[], ?::bigint[], ?::int[],
                           ?::boolean[], ?::timestamptz[], ?::int[], ?::bigint[])
      on conflict (event_id) do nothing
      returning event_id
      """;

  private static final String UPSERT_ROLLUPS = """
      insert into usage_rollup_minute as r (tenant_id, minute, model, requests, errors, tokens_in, tokens_out)
      select * from unnest(?::text[], ?::timestamptz[], ?::text[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[])
      on conflict (tenant_id, minute, model) do update set
        requests   = r.requests   + excluded.requests,
        errors     = r.errors     + excluded.errors,
        tokens_in  = r.tokens_in  + excluded.tokens_in,
        tokens_out = r.tokens_out + excluded.tokens_out
      """;

  private static final String UPSERT_MONTHS = """
      insert into tenant_month_usage as m (tenant_id, month, tokens, requests)
      select * from unnest(?::text[], ?::text[], ?::bigint[], ?::bigint[])
      on conflict (tenant_id, month) do update set
        tokens   = m.tokens   + excluded.tokens,
        requests = m.requests + excluded.requests
      returning tenant_id, month, tokens
      """;

  private final JdbcTemplate jdbc;

  public UsageStore(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Transactional
  public Result apply(List<Located> batch) {
    if (batch.isEmpty()) return new Result(0, 0, List.of());
    Set<UUID> inserted = insertEvents(batch);

    // Sorted maps give every transaction the same lock order, so concurrent batches cannot deadlock.
    Map<RollupKey, Rollup> rollups = new TreeMap<>(Comparator.comparing(RollupKey::tenantId)
        .thenComparing(RollupKey::minute).thenComparing(RollupKey::model));
    Map<MonthKey, Month> months = new TreeMap<>(Comparator.comparing(MonthKey::tenantId).thenComparing(MonthKey::month));
    Set<UUID> seen = new HashSet<>();
    for (Located l : batch) {
      UsageEvent e = l.event();
      if (!inserted.contains(e.eventId()) || !seen.add(e.eventId())) continue;
      Rollup r = rollups.computeIfAbsent(
          new RollupKey(e.tenantId(), e.occurredAt().truncatedTo(ChronoUnit.MINUTES), e.model()), k -> new Rollup());
      r.requests++;
      if (e.status() / 100 != 2) r.errors++;
      r.tokensIn += e.tokensIn();
      r.tokensOut += e.tokensOut();
      Month m = months.computeIfAbsent(new MonthKey(e.tenantId(), Topics.month(e.occurredAt())), k -> new Month());
      m.tokens += e.totalTokens();
      m.requests++;
    }
    upsertRollups(rollups);
    List<MonthTotal> totals = upsertMonths(months);
    return new Result(inserted.size(), batch.size() - inserted.size(), totals);
  }

  private Set<UUID> insertEvents(List<Located> batch) {
    int n = batch.size();
    Object[] ids = new Object[n], tenants = new Object[n], models = new Object[n], tin = new Object[n],
        tout = new Object[n], lat = new Object[n], status = new Object[n], streamed = new Object[n],
        at = new Object[n], part = new Object[n], off = new Object[n];
    for (int i = 0; i < n; i++) {
      Located l = batch.get(i);
      UsageEvent e = l.event();
      ids[i] = e.eventId().toString();
      tenants[i] = e.tenantId();
      models[i] = e.model();
      tin[i] = e.tokensIn();
      tout[i] = e.tokensOut();
      lat[i] = e.latencyMs();
      status[i] = e.status();
      streamed[i] = e.streamed();
      at[i] = e.occurredAt().toString();
      part[i] = l.partition();
      off[i] = l.offset();
    }
    return jdbc.execute((Connection c) -> {
      try (PreparedStatement ps = c.prepareStatement(INSERT_EVENTS)) {
        bind(c, ps, new String[] {"uuid", "text", "text", "int4", "int4", "int8", "int4", "bool", "timestamptz", "int4", "int8"},
            ids, tenants, models, tin, tout, lat, status, streamed, at, part, off);
        Set<UUID> out = new HashSet<>();
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) out.add(rs.getObject(1, UUID.class));
        }
        return out;
      }
    });
  }

  private void upsertRollups(Map<RollupKey, Rollup> rollups) {
    if (rollups.isEmpty()) return;
    int n = rollups.size();
    Object[] tenants = new Object[n], minutes = new Object[n], models = new Object[n], req = new Object[n],
        err = new Object[n], tin = new Object[n], tout = new Object[n];
    int i = 0;
    for (var en : rollups.entrySet()) {
      tenants[i] = en.getKey().tenantId();
      minutes[i] = en.getKey().minute().toString();
      models[i] = en.getKey().model();
      req[i] = en.getValue().requests;
      err[i] = en.getValue().errors;
      tin[i] = en.getValue().tokensIn;
      tout[i] = en.getValue().tokensOut;
      i++;
    }
    jdbc.execute((Connection c) -> {
      try (PreparedStatement ps = c.prepareStatement(UPSERT_ROLLUPS)) {
        bind(c, ps, new String[] {"text", "timestamptz", "text", "int8", "int8", "int8", "int8"},
            tenants, minutes, models, req, err, tin, tout);
        ps.executeUpdate();
        return null;
      }
    });
  }

  private List<MonthTotal> upsertMonths(Map<MonthKey, Month> months) {
    if (months.isEmpty()) return List.of();
    int n = months.size();
    Object[] tenants = new Object[n], ms = new Object[n], tokens = new Object[n], req = new Object[n];
    int i = 0;
    for (var en : months.entrySet()) {
      tenants[i] = en.getKey().tenantId();
      ms[i] = en.getKey().month();
      tokens[i] = en.getValue().tokens;
      req[i] = en.getValue().requests;
      i++;
    }
    return jdbc.execute((Connection c) -> {
      try (PreparedStatement ps = c.prepareStatement(UPSERT_MONTHS)) {
        bind(c, ps, new String[] {"text", "text", "int8", "int8"}, tenants, ms, tokens, req);
        List<MonthTotal> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) out.add(new MonthTotal(rs.getString(1), rs.getString(2), rs.getLong(3)));
        }
        return out;
      }
    });
  }

  private static void bind(Connection c, PreparedStatement ps, String[] types, Object[]... columns) throws SQLException {
    for (int i = 0; i < columns.length; i++) {
      Array a = c.createArrayOf(types[i], columns[i]);
      ps.setArray(i + 1, a);
    }
  }

  public List<MonthTotal> monthTotals(String month) {
    return jdbc.query("select tenant_id, month, tokens from tenant_month_usage where month = ?",
        (rs, n) -> new MonthTotal(rs.getString(1), rs.getString(2), rs.getLong(3)), month);
  }
}
