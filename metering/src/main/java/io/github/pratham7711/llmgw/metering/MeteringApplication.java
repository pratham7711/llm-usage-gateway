package io.github.pratham7711.llmgw.metering;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MeteringApplication {
  public static void main(String[] args) {
    SpringApplication.run(MeteringApplication.class, args);
  }
}
