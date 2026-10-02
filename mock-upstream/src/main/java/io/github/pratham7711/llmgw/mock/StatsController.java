package io.github.pratham7711.llmgw.mock;

import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsController {

  private final RequestLog log;

  public StatsController(RequestLog log) {
    this.log = log;
  }

  @PostMapping("/stats/reset")
  public Map<String, Object> reset(@RequestParam(defaultValue = "false") boolean record) {
    log.reset(record);
    return Map.of("recording", record);
  }

  @GetMapping("/stats/served")
  public Map<String, Object> served() {
    return Map.of("requests", log.requests(), "tokens", log.tokens());
  }

  /** One line per served request: "request-id prompt-tokens completion-tokens". */
  @GetMapping(path = "/stats/request-ids", produces = MediaType.TEXT_PLAIN_VALUE)
  public String requestIds() {
    StringBuilder out = new StringBuilder();
    log.snapshot().forEach((id, s) ->
        out.append(id).append(' ').append(s.promptTokens()).append(' ').append(s.completionTokens()).append('\n'));
    return out.toString();
  }
}
