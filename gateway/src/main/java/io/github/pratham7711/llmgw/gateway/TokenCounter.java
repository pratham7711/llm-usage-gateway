package io.github.pratham7711.llmgw.gateway;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * Counts tokens locally with OpenAI's BPE vocabularies, so the gateway can price a request before
 * forwarding it and can still bill a response that arrives without a usage report.
 *
 * For OpenAI models the vocabulary is the provider's own, so the counts match except for the
 * per-message framing estimated below. For other models it is an approximation; how far off it is
 * for a given model is measured, not assumed (see docs/DESIGN.md, "Counting tokens").
 */
@Component
public class TokenCounter {

  /** Framing tokens OpenAI adds around every chat message, and once to prime the reply. */
  static final int PER_MESSAGE = 3;
  static final int REPLY_PRIMING = 3;
  /** A low-detail image costs a flat 85 tokens; high-detail images cost more and are not estimated. */
  static final int PER_IMAGE = 85;

  private final Encoding o200k;
  private final Encoding cl100k;

  public TokenCounter() {
    EncodingRegistry registry = Encodings.newLazyEncodingRegistry();
    this.o200k = registry.getEncoding(EncodingType.O200K_BASE);
    this.cl100k = registry.getEncoding(EncodingType.CL100K_BASE);
  }

  /** Input tokens of a chat completions request body. */
  public int prompt(String model, JsonNode request) {
    Encoding enc = encodingFor(model);
    int total = REPLY_PRIMING;
    for (JsonNode m : request.path("messages")) {
      total += PER_MESSAGE + count(enc, m.path("role").asString(""));
      if (m.hasNonNull("name")) total += 1 + count(enc, m.path("name").asString(""));
      JsonNode content = m.path("content");
      if (content.isString()) {
        total += count(enc, content.asString());
      } else if (content.isArray()) {
        for (JsonNode part : content) {
          String type = part.path("type").asString("");
          if (type.equals("text")) total += count(enc, part.path("text").asString(""));
          else if (type.equals("image_url")) total += PER_IMAGE;
        }
      }
      for (JsonNode call : m.path("tool_calls")) {
        total += count(enc, call.path("function").path("name").asString(""));
        total += count(enc, call.path("function").path("arguments").asString(""));
      }
    }
    // Tool schemas are rendered into the prompt in a format OpenAI does not publish; their JSON
    // is a close, slightly high stand-in.
    if (request.path("tools").isArray() && !request.path("tools").isEmpty()) {
      total += count(enc, request.path("tools").toString());
    }
    return total;
  }

  public int text(String model, String text) {
    return count(encodingFor(model), text);
  }

  Encoding encodingFor(String model) {
    String m = model == null ? "" : model.toLowerCase();
    boolean older = (m.startsWith("gpt-4") && !m.startsWith("gpt-4o") && !m.startsWith("gpt-4.")) || m.startsWith("gpt-3.5");
    return older ? cl100k : o200k;
  }

  private static int count(Encoding enc, String s) {
    // Ordinary: text that happens to contain "<|endoftext|>" is counted as text, not rejected.
    return s.isEmpty() ? 0 : enc.countTokensOrdinary(s);
  }
}
