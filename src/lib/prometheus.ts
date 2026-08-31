import client from "prom-client";

const register = new client.Registry();

register.setDefaultLabels({ app: "heliobond-backend" });
client.collectDefaultMetrics({ register });

// ── HTTP metrics ────────────────────────────────────────────────────────────
export const httpRequestDuration = new client.Histogram({
  name: "http_request_duration_seconds",
  help: "Duration of HTTP requests in seconds",
  labelNames: ["method", "route", "status_code"] as const,
  buckets: [0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5],
  registers: [register],
});

export const httpRequestsTotal = new client.Counter({
  name: "http_requests_total",
  help: "Total number of HTTP requests",
  labelNames: ["method", "route", "status_code"] as const,
  registers: [register],
});

// ── Stellar RPC metrics ─────────────────────────────────────────────────────
export const stellarRpcDuration = new client.Histogram({
  name: "stellar_rpc_call_duration_seconds",
  help: "Duration of Stellar RPC calls in seconds",
  labelNames: ["operation"] as const,
  buckets: [0.1, 0.5, 1, 2, 5, 10, 30],
  registers: [register],
});

export const stellarRpcTotal = new client.Counter({
  name: "stellar_rpc_calls_total",
  help: "Total Stellar RPC calls",
  labelNames: ["operation", "result"] as const,
  registers: [register],
});

// ── Cron job metrics ────────────────────────────────────────────────────────
export const cronJobDuration = new client.Histogram({
  name: "cron_job_duration_seconds",
  help: "Duration of cron job executions in seconds",
  labelNames: ["job"] as const,
  buckets: [0.5, 1, 5, 10, 30, 60, 300],
  registers: [register],
});

export const cronJobTotal = new client.Counter({
  name: "cron_job_runs_total",
  help: "Total cron job executions",
  labelNames: ["job", "result"] as const,
  registers: [register],
});

// ── Transaction metrics ─────────────────────────────────────────────────────
export const txSubmissionTotal = new client.Counter({
  name: "stellar_tx_submissions_total",
  help: "Total Stellar transaction submissions",
  labelNames: ["result"] as const,
  registers: [register],
});

// ── Circuit breaker metrics ─────────────────────────────────────────────────
export const circuitBreakerState = new client.Gauge({
  name: "circuit_breaker_state",
  help: "Circuit breaker state (0=CLOSED, 1=HALF_OPEN, 2=OPEN)",
  labelNames: ["name"] as const,
  registers: [register],
});

export { register };
