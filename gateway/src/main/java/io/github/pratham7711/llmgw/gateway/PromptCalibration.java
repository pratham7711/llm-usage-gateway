package io.github.pratham7711.llmgw.gateway;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Learns, per model, how many more prompt tokens the provider counts than the gateway reserves.
 *
 * For OpenAI models the gateway's count matches the provider's to within the message framing.
 * Other providers wrap the prompt in their own chat template, often with a default system prompt,
 * which adds a roughly fixed number of tokens to every request: qwen2.5 on Ollama reported 30
 * prompt tokens for a one-word message the gateway counts as 8. A ratio would learn "3.75x" from
 * that and over-reserve a long prompt by the same factor, so the excess is learned in tokens.
 *
 * It jumps straight to any larger excess it sees, because reserving too little is what can spend
 * past a quota, and decays slowly when later requests need less. It never goes below zero, so a
 * reservation is never smaller than the local count plus its margin.
 */
@Component
public class PromptCalibration {

  static final int MAX_MODELS = 1000;
  static final int MAX_EXCESS = 8192;
  private static final double FALL = 0.01;

  /** Model to excess tokens, as the bits of a double. Model names come from clients, so the map is capped. */
  private final ConcurrentHashMap<String, AtomicLong> excess = new ConcurrentHashMap<>();
  private final MeterRegistry meters;

  public PromptCalibration(MeterRegistry meters) {
    this.meters = meters;
  }

  /** Prompt tokens to reserve for a request whose prompt the gateway counted at {@code estimate}. */
  public int hold(String model, int estimate, double margin) {
    return reserved(estimate, margin) + excess(model);
  }

  public int excess(String model) {
    AtomicLong e = excess.get(model);
    return e == null ? 0 : (int) Math.ceil(Double.longBitsToDouble(e.get()));
  }

  public void observe(String model, int estimate, int reported, double margin) {
    if (estimate <= 0 || reported <= 0) return;
    double seen = Math.min(MAX_EXCESS, Math.max(0, reported - reserved(estimate, margin)));
    AtomicLong e = excess.get(model);
    if (e == null) {
      if (seen == 0 || excess.size() >= MAX_MODELS) return;
      e = excess.computeIfAbsent(model, m -> {
        AtomicLong fresh = new AtomicLong(Double.doubleToLongBits(0));
        meters.gauge("gateway.token.prompt.excess", Tags.of("model", m), fresh, a -> Double.longBitsToDouble(a.get()));
        return fresh;
      });
    }
    e.updateAndGet(bits -> {
      double cur = Double.longBitsToDouble(bits);
      double next = seen >= cur ? seen : cur + FALL * (seen - cur);
      // Once it has decayed below half a token it is gone, rather than rounding up to one forever.
      return Double.doubleToLongBits(next < 0.5 ? 0 : next);
    });
  }

  private static int reserved(int estimate, double margin) {
    // The epsilon keeps floating-point noise (100 * 1.1 = 110.00000000000001) from adding a token.
    return (int) Math.ceil(estimate * (1 + margin) - 1e-9);
  }
}
