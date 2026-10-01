// Holds many streaming completions open at once through the gateway. Real LLM responses take
// seconds, so what runs out first is open connections and memory, not requests per second. k6
// needs a VU (and its memory) per open request, so this client uses virtual threads instead.
//
//   java Streams.java <gateway-url> <streams> <ramp-seconds>
//
// Starts <streams> requests spread evenly over <ramp-seconds>, reads every SSE event, and prints
// one line of JSON: completed, failed, peak open at once, and time-to-first-event and total
// duration percentiles in milliseconds.
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class Streams {

  public static void main(String[] args) throws Exception {
    String base = args[0];
    int n = Integer.parseInt(args[1]);
    double rampSeconds = Double.parseDouble(args[2]);
    HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
    AtomicInteger open = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    AtomicInteger ok = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    Map<String, Integer> failures = new ConcurrentHashMap<>();
    long[] firstEvent = new long[n];
    long[] total = new long[n];
    Arrays.fill(firstEvent, -1);
    Arrays.fill(total, -1);

    long t0 = System.nanoTime();
    try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < n; i++) {
        int id = i;
        long startAt = t0 + (long) (rampSeconds * 1e9 * i / n);
        pool.submit(() -> {
          for (long wait; (wait = startAt - System.nanoTime()) > 0; ) LockSupport.parkNanos(wait);
          HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/v1/chat/completions"))
              .timeout(Duration.ofSeconds(300))
              .header("Content-Type", "application/json")
              .header("Authorization", "Bearer " + String.format("sk-dev-lt-%02d", id % 50 + 1))
              .POST(HttpRequest.BodyPublishers.ofString("{\"model\":\"mock-small\",\"stream\":true,\"max_tokens\":256,"
                  + "\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
              .build();
          long started = System.nanoTime();
          peak.accumulateAndGet(open.incrementAndGet(), Math::max);
          try {
            HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
            boolean done = false;
            try (BufferedReader in = new BufferedReader(new InputStreamReader(resp.body(), UTF_8))) {
              if (resp.statusCode() != 200) {
                failures.merge("http_" + resp.statusCode(), 1, Integer::sum);
                failed.incrementAndGet();
                return;
              }
              for (String line; (line = in.readLine()) != null; ) {
                if (!line.startsWith("data: ")) continue;
                if (firstEvent[id] < 0) firstEvent[id] = millisSince(started);
                if (line.equals("data: [DONE]")) done = true;
              }
            }
            if (done) {
              total[id] = millisSince(started);
              ok.incrementAndGet();
            } else {
              failures.merge("truncated", 1, Integer::sum);
              failed.incrementAndGet();
            }
          } catch (Exception e) {
            failures.merge(e.getClass().getSimpleName(), 1, Integer::sum);
            failed.incrementAndGet();
          } finally {
            open.decrementAndGet();
          }
        });
      }
    }
    long wall = millisSince(t0);
    System.out.printf("STREAMS_JSON {\"streams\":%d,\"ramp_s\":%s,\"completed\":%d,\"failed\":%d,\"failures\":%s,"
            + "\"peak_open\":%d,\"wall_ms\":%d,\"first_event_ms\":%s,\"total_ms\":%s}%n",
        n, rampSeconds, ok.get(), failed.get(), json(new TreeMap<>(failures)), peak.get(), wall,
        percentiles(firstEvent), percentiles(total));
  }

  private static long millisSince(long nanos) {
    return (System.nanoTime() - nanos) / 1_000_000;
  }

  private static String percentiles(long[] values) {
    long[] v = Arrays.stream(values).filter(x -> x >= 0).sorted().toArray();
    if (v.length == 0) return "null";
    return String.format("{\"p50\":%d,\"p90\":%d,\"p99\":%d,\"max\":%d}",
        v[(int) (v.length * 0.50)], v[(int) (v.length * 0.90)], v[Math.min(v.length - 1, (int) (v.length * 0.99))], v[v.length - 1]);
  }

  private static String json(Map<String, Integer> m) {
    StringBuilder sb = new StringBuilder("{");
    m.forEach((k, v) -> sb.append(sb.length() > 1 ? "," : "").append('"').append(k).append("\":").append(v));
    return sb.append('}').toString();
  }
}
