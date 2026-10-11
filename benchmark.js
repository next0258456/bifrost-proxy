import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '3s', target: 50 },  // Ramp up to 50 users
    { duration: '10s', target: 50 }, // Hold 50 users under heavy load
    { duration: '2s', target: 0 },   // Ramp down
  ],
  thresholds: {
    // Đảm bảo SLA cam kết trong Spec: p(99) < 15ms
    'http_req_duration': ['p(99)<15', 'avg<5'],
  },
};

export default function () {
  // 1. Test Auth Endpoint (Fake Token < 5ms)
  const authRes = http.post('http://bifrost-proxy:8080/v1/oauth/tokens', 'grant_type=client_credentials', {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });
  
  check(authRes, {
    'auth status is 200': (r) => r.status === 200,
    'auth latency < 5ms': (r) => r.timings.duration < 5,
  });

  // 2. Test Catalog Read GET
  const readRes = http.get('http://bifrost-proxy:8080/v1/namespaces/bronze/tables/fmcg_sales');
  
  check(readRes, {
    'read status is 200 or 404': (r) => r.status === 200 || r.status === 404,
  });

  // 3. Test Ingestion Commit POST (< 15ms SLA)
  const commitPayload = JSON.stringify({
    'requirements': [],
    'updates': [
      { 'action': 'set-current-snapshot', 'snapshot-id': 1001 }
    ],
    'metadata-location': 's3://lakehouse/bronze/fmcg_sales/metadata/v1.json'
  });

  const commitRes = http.post('http://bifrost-proxy:8080/v1/namespaces/bronze/tables/fmcg_sales', commitPayload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(commitRes, {
    'commit status is 200 or 409': (r) => r.status === 200 || r.status === 409,
    'commit latency < 15ms': (r) => r.timings.duration < 15,
  });

  sleep(0.05); // Simulated inter-arrival interval
}
