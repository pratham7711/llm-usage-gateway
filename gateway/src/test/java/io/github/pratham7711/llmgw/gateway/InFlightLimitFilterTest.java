package io.github.pratham7711.llmgw.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class InFlightLimitFilterTest {

  @Test
  void defaultLimitScalesWithAvailableCpus() {
    SimpleMeterRegistry meters = new SimpleMeterRegistry();
    InFlightLimitFilter filter = new InFlightLimitFilter(0, meters);

    int expected = InFlightLimitFilter.PERMITS_PER_CPU * Runtime.getRuntime().availableProcessors();
    assertThat(filter.limit()).isEqualTo(expected);
    assertThat(meters.get("gateway.inflight.limit").gauge().value()).isEqualTo(expected);
  }

  @Test
  void anExplicitLimitWins() {
    assertThat(new InFlightLimitFilter(300, new SimpleMeterRegistry()).limit()).isEqualTo(300);
  }
}
