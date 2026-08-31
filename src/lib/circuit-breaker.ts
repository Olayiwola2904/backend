import { logger } from "./logger";

export type BreakerState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface CircuitBreakerConfig {
  failureThreshold: number;
  recoveryTimeoutMs: number;
  name?: string;
}

export interface BreakerMetrics {
  state: BreakerState;
  consecutiveFailures: number;
  totalFailures: number;
  totalSuccesses: number;
  lastFailureAt: number | null;
  openedAt: number | null;
  lastStateChange: number;
}

export class CircuitBreaker {
  private state: BreakerState = "CLOSED";
  private consecutiveFailures = 0;
  private totalFailures = 0;
  private totalSuccesses = 0;
  private lastFailureAt: number | null = null;
  private openedAt: number | null = null;
  private lastStateChange = Date.now();
  private readonly config: Required<CircuitBreakerConfig>;

  constructor(config: Partial<CircuitBreakerConfig> = {}) {
    this.config = {
      failureThreshold: config.failureThreshold ?? 5,
      recoveryTimeoutMs: config.recoveryTimeoutMs ?? 30_000,
      name: config.name ?? "CircuitBreaker",
    };
  }

  /** Execute fn, applying circuit-breaker logic. Falls back to fallback() when OPEN. */
  async execute<T>(fn: () => Promise<T>, fallback?: () => Promise<T>): Promise<T> {
    if (this.state === "OPEN") {
      if (Date.now() - (this.openedAt ?? 0) >= this.config.recoveryTimeoutMs) {
        this.transition("HALF_OPEN");
      } else {
        if (fallback) return fallback();
        throw new Error(`[${this.config.name}] Circuit is OPEN – request rejected`);
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      if (fallback && this.state === "OPEN") return fallback();
      throw err;
    }
  }

  private onSuccess(): void {
    this.totalSuccesses++;
    this.consecutiveFailures = 0;
    if (this.state === "HALF_OPEN") {
      this.transition("CLOSED");
    }
  }

  private onFailure(): void {
    this.totalFailures++;
    this.consecutiveFailures++;
    this.lastFailureAt = Date.now();

    if (
      this.state === "HALF_OPEN" ||
      (this.state === "CLOSED" && this.consecutiveFailures >= this.config.failureThreshold)
    ) {
      this.openedAt = Date.now();
      this.transition("OPEN");
    }
  }

  private transition(next: BreakerState): void {
    logger.warn(`[${this.config.name}] circuit state: ${this.state} → ${next}`, {
      from: this.state,
      to: next,
      consecutiveFailures: this.consecutiveFailures,
    });
    this.state = next;
    this.lastStateChange = Date.now();
  }

  getState(): BreakerState {
    return this.state;
  }

  getMetrics(): BreakerMetrics {
    return {
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      totalFailures: this.totalFailures,
      totalSuccesses: this.totalSuccesses,
      lastFailureAt: this.lastFailureAt,
      openedAt: this.openedAt,
      lastStateChange: this.lastStateChange,
    };
  }

  /** Reset to CLOSED – useful for testing or manual recovery. */
  reset(): void {
    this.state = "CLOSED";
    this.consecutiveFailures = 0;
    this.openedAt = null;
    this.lastStateChange = Date.now();
  }
}
