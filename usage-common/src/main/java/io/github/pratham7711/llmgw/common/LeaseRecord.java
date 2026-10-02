package io.github.pratham7711.llmgw.common;

/**
 * The value stored for one lease in {@link Topics#leaseKey}. Two shapes, written by the Lua scripts:
 *
 * <pre>
 *   H|hold|promptHold|month|eventJson   admitted, not settled: tokens reserved against month's quota
 *   S|eventJson                         settled: the exact usage event the gateway is publishing
 * </pre>
 *
 * A held lease's event is a template billed at its reservation if the lease expires: the prompt
 * estimate as input and the output tokens granted at admission (hold - promptHold) as output.
 */
public record LeaseRecord(boolean settled, long hold, long promptHold, String month, String eventJson) {

  public static LeaseRecord parse(String value) {
    if (value.startsWith("S|")) return new LeaseRecord(true, 0, 0, null, value.substring(2));
    if (!value.startsWith("H|")) throw new IllegalArgumentException("not a lease record");
    String[] parts = value.split("\\|", 5);
    if (parts.length != 5) throw new IllegalArgumentException("truncated lease record");
    return new LeaseRecord(false, Long.parseLong(parts[1]), Long.parseLong(parts[2]), parts[3], parts[4]);
  }

  /** Output tokens the lease granted at admission. */
  public long grantedOutput() {
    return hold - promptHold;
  }
}
