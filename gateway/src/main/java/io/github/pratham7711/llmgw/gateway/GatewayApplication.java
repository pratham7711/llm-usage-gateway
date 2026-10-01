package io.github.pratham7711.llmgw.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class GatewayApplication {
  public static void main(String[] args) {
    // The JDK client pools idle upstream connections without a size cap and for 20 minutes by
    // default. It tracks idle connections in a linked list that is scanned on checkout, so after
    // a spike every request pays for the connections the spike left behind: a profile at 1000 rps
    // put 20% of CPU samples in ConnectionPool$ExpiryList.remove. Cap the pool, age them out fast.
    defaultProperty("jdk.httpclient.connectionPoolSize", "256");
    defaultProperty("jdk.httpclient.keepalive.timeout", "30");
    SpringApplication.run(GatewayApplication.class, args);
  }

  private static void defaultProperty(String key, String value) {
    if (System.getProperty(key) == null) System.setProperty(key, value);
  }
}
