package io.github.pratham7711.llmgw.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class TokenCounterTest {

  private final TokenCounter tokens = new TokenCounter();
  private final JsonMapper json = JsonMapper.builder().build();

  private int prompt(String model, String body) {
    return tokens.prompt(model, json.readTree(body));
  }

  @Test
  void countsChatFramingAroundEveryMessage() {
    // 3 to prime the reply, then 3 per message plus its role and content: "user" and "hi" are one token each.
    assertThat(prompt("gpt-4o-mini", "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
        .isEqualTo(FakeUpstream.HI_PROMPT_TOKENS);
    assertThat(prompt("gpt-4o-mini",
        "{\"messages\":[{\"role\":\"system\",\"content\":\"hi\"},{\"role\":\"user\",\"content\":\"hi\"}]}"))
        .isEqualTo(FakeUpstream.HI_PROMPT_TOKENS + 5);
  }

  @Test
  void countsTextPartsImagesAndTools() {
    int text = prompt("gpt-4o", "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hi\"}]}]}");
    assertThat(text).isEqualTo(FakeUpstream.HI_PROMPT_TOKENS);
    int image = prompt("gpt-4o", "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hi\"},"
        + "{\"type\":\"image_url\",\"image_url\":{\"url\":\"https://example.com/a.png\"}}]}]}");
    assertThat(image).isEqualTo(text + TokenCounter.PER_IMAGE);
    int tools = prompt("gpt-4o", "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],"
        + "\"tools\":[{\"type\":\"function\",\"function\":{\"name\":\"get_weather\",\"parameters\":{}}}]}");
    assertThat(tools).isGreaterThan(text + 5);
  }

  @Test
  void picksTheVocabularyByModelFamily() {
    assertThat(tokens.encodingFor("gpt-4o-mini").getName()).isEqualTo("o200k_base");
    assertThat(tokens.encodingFor("gpt-4.1").getName()).isEqualTo("o200k_base");
    assertThat(tokens.encodingFor("gpt-4-turbo").getName()).isEqualTo("cl100k_base");
    assertThat(tokens.encodingFor("gpt-3.5-turbo").getName()).isEqualTo("cl100k_base");
    assertThat(tokens.encodingFor("qwen2.5:0.5b").getName()).isEqualTo("o200k_base");
  }

  @Test
  void countsSpecialTokenTextAsOrdinaryText() {
    assertThat(tokens.text("gpt-4o", "<|endoftext|>")).isGreaterThan(1);
    assertThat(tokens.text("gpt-4o", "")).isZero();
  }
}
