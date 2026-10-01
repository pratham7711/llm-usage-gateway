import http from 'k6/http';
import { Counter } from 'k6/metrics';

export const ok = new Counter('ok_responses');
export const rejected = new Counter('non2xx_responses');

const BASE = __ENV.GATEWAY_URL || 'http://localhost:8080';
const TENANTS = Array.from({ length: 50 }, (_, i) => `lt-${String(i + 1).padStart(2, '0')}`);
const PROMPTS = [
  'Summarise last week of campaign performance in three bullet points.',
  'Draft a polite follow-up to a creator who has not posted yet.',
  'Which posts underperformed on engagement and why might that be?',
  'Rewrite this brief so it fits in one paragraph.',
];

export function chat(stream) {
  const tenant = TENANTS[Math.floor(Math.random() * TENANTS.length)];
  const body = JSON.stringify({
    model: 'mock-small',
    stream,
    max_tokens: 256,
    messages: [{ role: 'user', content: PROMPTS[Math.floor(Math.random() * PROMPTS.length)] }],
  });
  const res = http.post(`${BASE}/v1/chat/completions`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer sk-dev-${tenant}` },
    timeout: '30s',
    tags: { name: stream ? 'chat_stream' : 'chat' },
  });
  if (res.status === 200) ok.add(1);
  else rejected.add(1);
  return res;
}

export function summary(name, data) {
  const d = data.metrics.http_req_duration?.values || {};
  const okN = data.metrics.ok_responses?.values.count || 0;
  const bad = data.metrics.non2xx_responses?.values.count || 0;
  const dropped = data.metrics.dropped_iterations?.values.count || 0;
  const rps = data.metrics.http_reqs?.values.rate || 0;
  const line = `${name}: rps=${rps.toFixed(0)} ok=${okN} non2xx=${bad} dropped=${dropped} ` +
    `p50=${(d.med || 0).toFixed(1)}ms p95=${(d['p(95)'] || 0).toFixed(1)}ms p99=${(d['p(99)'] || 0).toFixed(1)}ms max=${(d.max || 0).toFixed(0)}ms\n`;
  // In a Kubernetes Job there is no volume to read the file back from, so the summary can also go
  // to stdout on one marked line.
  const stdout = __ENV.SUMMARY_STDOUT === 'json' ? `${line}K6_SUMMARY_JSON ${JSON.stringify(data)}\n` : line;
  return { stdout, [`/results/${name}.json`]: JSON.stringify(data, null, 1) };
}
