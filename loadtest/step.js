// One constant-arrival-rate step: RATE requests/s for DURATION. A step "holds" when nothing is
// dropped (k6 always had a free VU) and the error rate stays at zero.
import { chat, summary } from './common.js';

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  // Referencing the submetric makes k6 report latency of successful responses separately from
  // fast 503s the gateway sheds under overload.
  thresholds: { 'http_req_duration{expected_response:true}': ['max>=0'] },
  scenarios: {
    step: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 500),
      timeUnit: '1s',
      duration: __ENV.DURATION || '45s',
      preAllocatedVUs: Number(__ENV.VUS || 2000),
      maxVUs: Number(__ENV.MAX_VUS || 5000),
    },
  },
};

const streamShare = Number(__ENV.STREAM_SHARE || 0.2);
export default function () {
  chat(Math.random() < streamShare);
}

export function handleSummary(data) {
  return summary(__ENV.NAME || `step-${__ENV.RATE}`, data);
}
