package io.github.pratham7711.llmgw.gateway;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Executors;
import org.springframework.stereotype.Component;

/**
 * Calls the upstream provider with the gateway's own credential. A tenant's key is never forwarded.
 * Blocking calls are cheap here because every request runs on a virtual thread.
 *
 * Each call carries the request's lease id as X-Request-Id, the same id its usage event is billed
 * under, so a provider-side log can be reconciled against billing row by row.
 */
@Component
public class UpstreamClient {

  private final HttpClient http;
  private final GatewayProperties props;
  private final URI completions;

  public UpstreamClient(GatewayProperties props) {
    this.props = props;
    this.completions = URI.create(props.upstreamBaseUrl().replaceAll("/+$", "") + "/v1/chat/completions");
    this.http = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(props.upstreamConnectTimeout())
        .executor(Executors.newVirtualThreadPerTaskExecutor())
        .build();
  }

  public HttpResponse<byte[]> send(byte[] body, String requestId) throws IOException, InterruptedException {
    return http.send(request(body, requestId), HttpResponse.BodyHandlers.ofByteArray());
  }

  public HttpResponse<InputStream> stream(byte[] body, String requestId) throws IOException, InterruptedException {
    return http.send(request(body, requestId), HttpResponse.BodyHandlers.ofInputStream());
  }

  private HttpRequest request(byte[] body, String requestId) {
    HttpRequest.Builder b = HttpRequest.newBuilder(completions)
        .timeout(props.upstreamRequestTimeout())
        .header("Content-Type", "application/json")
        .header("X-Request-Id", requestId)
        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
    if (props.upstreamApiKey() != null && !props.upstreamApiKey().isBlank()) {
      b.header("Authorization", "Bearer " + props.upstreamApiKey());
    }
    return b.build();
  }
}
