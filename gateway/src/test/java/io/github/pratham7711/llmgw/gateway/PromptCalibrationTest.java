package io.github.pratham7711.llmgw.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class PromptCalibrationTest {

  private static final double MARGIN = 0.10;
  private final PromptCalibration calibration = new PromptCalibration(new SimpleMeterRegistry());

  @Test
  void reservesTheLocalCountPlusMarginForAnUnseenModel() {
    assertThat(calibration.excess("new-model")).isZero();
    assertThat(calibration.hold("new-model", 100, MARGIN)).isEqualTo(110);
  }

  @Test
  void learnsAChatTemplatesFixedOverheadAtOnce() {
    // qwen2.5 on Ollama: 30 prompt tokens for a message the gateway counts as 8.
    calibration.observe("qwen", 8, 30, MARGIN);
    assertThat(calibration.hold("qwen", 8, MARGIN)).isGreaterThanOrEqualTo(30);
    // The overhead is fixed, so a long prompt is not over-reserved by the short prompt's ratio.
    assertThat(calibration.hold("qwen", 1000, MARGIN)).isEqualTo(1100 + 21);
  }

  @Test
  void decaysSlowlyAndNeverBelowZero() {
    calibration.observe("m", 100, 150, MARGIN);
    int high = calibration.excess("m");
    assertThat(high).isEqualTo(40);
    calibration.observe("m", 100, 100, MARGIN);
    assertThat(calibration.excess("m")).isBetween(high - 1, high);
    for (int i = 0; i < 5000; i++) calibration.observe("m", 100, 50, MARGIN);
    assertThat(calibration.excess("m")).isZero();
  }

  @Test
  void ignoresProvidersThatCountNoMoreThanTheReservation() {
    for (int i = 0; i < 10; i++) calibration.observe("gpt-4o-mini", 100, 104, MARGIN);
    assertThat(calibration.excess("gpt-4o-mini")).isZero();
  }

  @Test
  void boundsTheExcessAndTheNumberOfModels() {
    calibration.observe("wild", 1, 1_000_000, MARGIN);
    assertThat(calibration.excess("wild")).isEqualTo(PromptCalibration.MAX_EXCESS);
    for (int i = 0; i < PromptCalibration.MAX_MODELS + 50; i++) calibration.observe("m-" + i, 10, 50, MARGIN);
    assertThat(calibration.excess("m-" + (PromptCalibration.MAX_MODELS + 10))).isZero();
    calibration.observe("ignored", 0, 10, MARGIN);
    assertThat(calibration.excess("ignored")).isZero();
  }
}
