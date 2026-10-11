import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '5s', target: 50 }, // Ramp up to 50 users
    { duration: '15s', target: 50 }, // Hold 50 users
    { duration: '5s', target: 0 },  // Ramp down
  ],
  thresholds: {
    // Đảm bảo SLA khắt khe
    'http_req_duration': ['p(99)<10', 'avg<2'], // Max tail latency < 10ms, avg ping < 2ms
  },
};

export default function () {
  // 1. Test Auth Endpoint (Fake Token)
  const authRes = http.post('http://bifrost-proxy:8080/v1/oauth/tokens', 'grant_type=client_credentials', {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });
  
  check(authRes, {
    'auth status is 200': (r) => r.status === 200,
    'auth latency < 5ms': (r) => r.timings.duration < 5,
  });

  // 2. Test Catalog Read
  const readRes = http.get('http://bifrost-proxy:8080/v1/namespaces/default/tables/fmcg_sales');
  
  check(readRes, {
    // Expecting 404 because table doesn't exist yet, but we just measure latency
    'read status is 404': (r) => r.status === 404, 
  });

  sleep(0.1); // Small sleep to simulate realistic load
}
