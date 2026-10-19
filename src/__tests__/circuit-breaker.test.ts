import { CircuitBreaker } from "../lib/circuit-breaker";

jest.mock("../lib/logger", () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    formatError: jest.fn((err: unknown) => ({ error: String(err) })),
  },
}));

describe("CircuitBreaker", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("opens the circuit after consecutive failures", async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 2 });
    const failingCall = jest.fn().mockRejectedValue(new Error("boom"));

    await expect(breaker.execute(failingCall)).rejects.toThrow("boom");
    await expect(breaker.execute(failingCall)).rejects.toThrow("boom");

    expect(breaker.getState()).toBe("OPEN");
    expect(breaker.getMetrics().consecutiveFailures).toBe(2);
  });

  it("rejects requests immediately when the circuit is open", async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1 });
    const failingCall = jest.fn().mockRejectedValue(new Error("boom"));

    await expect(breaker.execute(failingCall)).rejects.toThrow("boom");

    const blockedCall = jest.fn().mockResolvedValue("should-not-run");
    await expect(breaker.execute(blockedCall)).rejects.toThrow("Circuit is OPEN");

    expect(blockedCall).not.toHaveBeenCalled();
  });

  it("half-opens after the cooldown window elapses", async () => {
    let currentTime = 0;
    jest.spyOn(Date, "now").mockImplementation(() => currentTime);

    const breaker = new CircuitBreaker({ failureThreshold: 1, recoveryTimeoutMs: 1000 });
    const failingCall = jest.fn().mockRejectedValue(new Error("boom"));

    await expect(breaker.execute(failingCall)).rejects.toThrow("boom");

    currentTime = 1001;

    let observedState: string | undefined;
    const successCall = jest.fn().mockImplementation(async () => {
      observedState = breaker.getState();
      return "ok";
    });

    await expect(breaker.execute(successCall)).resolves.toBe("ok");
    expect(observedState).toBe("HALF_OPEN");
  });

  it("logs state transitions via the structured logger", async () => {
    const { logger } = jest.requireMock("../lib/logger") as { logger: { warn: jest.Mock } };
    const breaker = new CircuitBreaker({ failureThreshold: 1, name: "TestRPC" });
    const failingCall = jest.fn().mockRejectedValue(new Error("boom"));

    await expect(breaker.execute(failingCall)).rejects.toThrow("boom");
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining("TestRPC"),
      expect.objectContaining({ from: "CLOSED", to: "OPEN" }),
    );
  });

  it("uses configurable threshold from CIRCUIT_BREAKER_THRESHOLD env var", () => {
    const breaker = new CircuitBreaker({ failureThreshold: 10 });
    expect(breaker.getMetrics().state).toBe("CLOSED");
  });

  it("closes the circuit after a successful request", async () => {
    let currentTime = 0;
    jest.spyOn(Date, "now").mockImplementation(() => currentTime);

    const breaker = new CircuitBreaker({ failureThreshold: 1, recoveryTimeoutMs: 1000 });
    const failingCall = jest.fn().mockRejectedValue(new Error("boom"));

    await expect(breaker.execute(failingCall)).rejects.toThrow("boom");

    currentTime = 1001;
    await expect(breaker.execute(async () => "ok")).resolves.toBe("ok");

    expect(breaker.getState()).toBe("CLOSED");
    expect(breaker.getMetrics().consecutiveFailures).toBe(0);
  });
});
