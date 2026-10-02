package io.github.pratham7711.llmgw.metering;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Model prices from {@code model_price}, held in memory and reloaded periodically. A request is
 * priced at the row in force when it occurred, so a price change never reprices old usage.
 * Cost is kept in nano-USD (1e-9 USD) as a long: exact for any per-token price with up to three
 * decimals per million tokens, and summable without floating-point drift.
 */
@Component
public class PriceBook {

  private static final Logger log = LoggerFactory.getLogger(PriceBook.class);
  /** USD per million tokens to nano-USD per token. */
  private static final BigDecimal NANO_PER_MTOK = BigDecimal.valueOf(1000);

  record Price(Instant from, long inNanoPerToken, long outNanoPerToken) {}

  private final JdbcTemplate jdbc;
  private final Counter unpriced;
  /** Per model, newest first. Replaced whole on reload, never mutated. */
  private volatile Map<String, List<Price>> prices = Map.of();
  private volatile boolean loaded;

  public PriceBook(JdbcTemplate jdbc, MeterRegistry meters) {
    this.jdbc = jdbc;
    this.unpriced = meters.counter("metering.cost.unpriced");
  }

  @Scheduled(fixedDelayString = "${metering.price-reload-interval:60s}")
  public void reload() {
    try {
      Map<String, List<Price>> next = new HashMap<>();
      jdbc.query("""
          select model, effective_from, input_usd_per_mtok, output_usd_per_mtok
          from model_price order by model, effective_from desc
          """, rs -> {
            next.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(new Price(
                rs.getTimestamp(2).toInstant(), nano(rs.getBigDecimal(3)), nano(rs.getBigDecimal(4))));
          });
      prices = next;
      loaded = true;
    } catch (RuntimeException e) {
      log.warn("price reload failed, keeping {} models: {}", prices.size(), e.toString());
    }
  }

  /** Cost of one request in nano-USD; 0 for a model with no price in force (counted as unpriced). */
  public long costNanoUsd(String model, Instant at, long tokensIn, long tokensOut) {
    if (!loaded) reload();
    for (Price p : prices.getOrDefault(model, List.of())) {
      if (!p.from().isAfter(at)) {
        return Math.addExact(Math.multiplyExact(tokensIn, p.inNanoPerToken()),
            Math.multiplyExact(tokensOut, p.outNanoPerToken()));
      }
    }
    unpriced.increment();
    return 0;
  }

  private static long nano(BigDecimal usdPerMtok) {
    return usdPerMtok.multiply(NANO_PER_MTOK).setScale(0, RoundingMode.HALF_UP).longValueExact();
  }
}
