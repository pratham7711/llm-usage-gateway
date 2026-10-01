// Baseline load with three 10-second bursts at 10x.
import { chat, summary } from './common.js';

const base = Number(__ENV.BASE_RATE || 300);
const peak = Number(__ENV.PEAK_RATE || 3000);
const cycle = [
  { target: base, duration: '30s' },
  { target: peak, duration: '3s' },
  { target: peak, duration: '10s' },
  { target: base, duration: '3s' },
];

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: { 'http_req_duration{expected_response:true}': ['max>=0'] },
  scenarios: {
    burst: {
      executor: 'ramping-arrival-rate',
      startRate: base,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 5000,
      stages: [...cycle, ...cycle, ...cycle, { target: base, duration: '30s' }],
    },
  },
};

export default function () {
  chat(Math.random() < 0.2);
}

export function handleSummary(data) {
  return summary(__ENV.NAME || 'burst', data);
}
