import { Prisma, PrismaClient } from '@prisma/client';
import { BaseService } from '../../services/base.service';
import { AppError, NotFoundError, UnauthorizedError, ValidationError, DuplicatePaymentError, ForbiddenError, InsufficientFundsError, InvalidCurrencyError, PaymentDeclinedError, PaymentNotFoundError } from '../../utils/errors';
import { logger } from '../../utils/logger';
import { PaymentProvider, createPaymentProvider, resolveWebhookSecret, resolveWebhookToleranceSeconds } from '../../lib/payments/provider';
import { isRetryableProviderError, retryOperation, RetryOptions } from '../../lib/payments/retry';
import { PAYMENT_EVENT_TYPES, canTransition, transitionForWebhookEvent } from '../../lib/payments/transitions';
import { PaymentStatus, isRefundablePaymentStatus } from '../../lib/payments/types';
import { verifyWebhookSignature } from '../../lib/payments/webhook-signature';
import { TransactionService } from '../../services/transaction.service';
import {
  CreatePaymentInput,
  ListPaymentsQueryInput,
  PaymentListResponse,
  PaymentResponse,
  RefundPaymentInput,
  RefundResponse,
  WebhookResult,
} from './charge.types';

export interface ChargeServiceOptions {
  provider?: PaymentProvider;
  retry?: RetryOptions;
  webhookSecret?: string;
  webhookToleranceSeconds?: number;
  transactions?: TransactionService;
}

/**
 * Payment processing service.
 *
 * Responsibilities:
 * - create tokenized payment intents through the configured provider
 * - idempotent creation/refunds keyed by an idempotency key
 * - status tracking with an append-only audit trail
 * - refunds with over-refund protection
 * - verified, idempotent webhook processing (including chargebacks)
 *
 * Raw card data is never accepted or stored — only provider references.
 */
export class ChargeService extends BaseService {
  private readonly provider: PaymentProvider;
  private readonly retry: RetryOptions;
  private readonly webhookSecret?: string;
  private readonly webhookToleranceSeconds: number;
  private readonly transactions: TransactionService;

  constructor(
    private readonly prisma: PrismaClient,
    options: ChargeServiceOptions = {}
  ) {
    super();
    this.provider = options.provider ?? createPaymentProvider();
    this.retry = options.retry ?? { attempts: 3, sleep: async () => undefined };
    this.webhookSecret = options.webhookSecret ?? resolveWebhookSecret();
    this.webhookToleranceSeconds = options.webhookToleranceSeconds ?? resolveWebhookToleranceSeconds();
    this.transactions = options.transactions ?? new TransactionService(prisma);
  }

  get providerName(): string {
    return this.provider.name;
  }

  /**
   * Creates (or returns the existing) payment for an idempotency key.
   *
   * #175 — No on-chain balance check is performed here. ChargeService processes
   * tokenized card payments through an external provider (e.g. Stripe); there is
   * no sender Stellar wallet and no on-chain balance to verify. The pre-flight
   * balance check required by #175 applies only to Stellar-based tip flows in
   * PaymentService.createTip(), where a wallet public key and getWalletBalance()
   * are available. Card payment failures are handled by the provider and surfaced
   * via webhook status transitions.
   */
  async createPayment(userId: string, input: CreatePaymentInput, idempotencyKey: string): Promise<PaymentResponse> {
    return this.executeWithLogging('payments.create', async () => {
      const existing = await this.prisma.payment.findUnique({ where: { idempotencyKey }, include: { refunds: true } });
      if (existing) {
        if (existing.userId !== userId) {
          logger.warn({ userId, idempotencyKey, failureReason: 'idempotency_key_conflict' }, 'Payment creation rejected: idempotency key reused by different user');
          throw new DuplicatePaymentError('Idempotency key already used by another user', { idempotencyKey });
        }
        if (existing.amount !== input.amount) {
          logger.warn({ userId, idempotencyKey, failureReason: 'idempotency_key_conflict' }, 'Payment creation rejected: idempotency key reused with a different amount');
          throw new DuplicatePaymentError('Idempotency key already used with a different amount', { idempotencyKey });
        }
        const incomingCreatorId = input.creatorId ?? null;
        if (existing.creatorId !== incomingCreatorId) {
          logger.warn({ userId, idempotencyKey, failureReason: 'idempotency_key_conflict' }, 'Payment creation rejected: idempotency key reused with a different creator');
          throw new DuplicatePaymentError('Idempotency key already used with a different creator', { idempotencyKey });
        }
        return this.toResponse(existing);
      }

      if (!Number.isFinite(input.amount) || input.amount <= 0) {
        throw new ValidationError('Amount must be greater than 0');
      }

      const intent = await retryOperation(
        () =>
          this.provider.createPaymentIntent({
            amount: input.amount,
            currency: input.currency,
            method: input.method,
            description: input.description,
            metadata: { userId, ...(input.creatorId ? { creatorId: input.creatorId } : {}) },
            idempotencyKey,
          }),
        { ...this.retry, shouldRetry: (error) => isRetryableProviderError(error) }
      );

      // Persist the payment and its creation event atomically via the transaction service.
      const payment = await this.transactions.createPaymentWithEvent(
        {
          idempotencyKey,
          userId,
          creatorId: input.creatorId ?? null,
          provider: this.provider.name,
          providerRef: intent.providerRef,
          method: input.method,
          amount: input.amount,
          currency: input.currency,
          status: intent.status,
          description: input.description ?? null,
        },
        {
          type: PAYMENT_EVENT_TYPES.CREATED,
          actorId: userId,
          payload: {
            provider: this.provider.name,
            providerRef: intent.providerRef,
            amount: input.amount,
            currency: input.currency,
            method: input.method,
          },
        }
      );

      logger.info({ paymentId: payment.id, provider: this.provider.name }, 'Payment created');

      return { ...this.toResponse(payment), clientSecret: intent.clientSecret };
    });
  }

  async getPayment(paymentId: string, userId?: string): Promise<PaymentResponse> {
    return this.executeWithLogging('payments.get', async () => {
      const payment = await this.prisma.payment.findUnique({ where: { id: paymentId }, include: { refunds: true } });
      if (!payment) {
        throw new NotFoundError('Payment');
      }
      if (userId && payment.userId !== userId) {
        logger.warn({ requestingUserId: userId, paymentId, failureReason: 'wrong_owner' }, 'Payment access denied: belongs to another user');
        throw new ForbiddenError('Payment belongs to another user');
      }
      return this.toResponse(payment);
    });
  }

  async listPayments(userId: string, query: ListPaymentsQueryInput = {}): Promise<PaymentListResponse> {
    return this.executeWithLogging('payments.list', async () => {
      const page = query.page ?? 1;
      const pageSize = query.pageSize ?? 20;
      const where: Record<string, unknown> = { userId };
      if (query.status) {
        where.status = query.status;
      }

      const [payments, total] = await Promise.all([
        this.prisma.payment.findMany({
          where,
          skip: (page - 1) * pageSize,
          take: pageSize,
          orderBy: { createdAt: 'desc' },
          include: { refunds: true },
        }),
        this.prisma.payment.count({ where }),
      ]);

      return {
        payments: payments.map((payment) => this.toResponse(payment)),
        total,
        page,
        pageSize,
        totalPages: Math.ceil(total / pageSize),
      };
    });
  }

  /**
   * Refunds a payment (fully when no amount is given). Idempotent per refund
   * idempotency key and protected against over-refunding.
   */
  async refundPayment(paymentId: string, userId: string, input: RefundPaymentInput, idempotencyKey: string): Promise<RefundResponse> {
    return this.executeWithLogging('payments.refund', async () => {
      const existingRefund = await this.prisma.refund.findUnique({ where: { idempotencyKey } });
      if (existingRefund) {
        return this.toRefundResponse(existingRefund);
      }

      const payment = await this.prisma.payment.findUnique({ where: { id: paymentId }, include: { refunds: true } });
      if (!payment) {
        throw new NotFoundError('Payment');
      }
      if (payment.userId !== userId) {
        logger.warn({ requestingUserId: userId, paymentId, failureReason: 'wrong_owner' }, 'Refund access denied: payment belongs to another user');
        throw new ForbiddenError('Payment belongs to another user');
      }
      if (!isRefundablePaymentStatus(payment.status)) {
        logger.warn({ userId, paymentId, paymentStatus: payment.status, failureReason: 'non_refundable_status' }, 'Refund rejected: payment status is not refundable');
        throw new ValidationError(`Payment in status "${payment.status}" cannot be refunded`);
      }
      if (!payment.providerRef) {
        throw new ValidationError('Payment has no provider reference to refund');
      }

      const alreadyRefunded = this.sumRefunded(payment.refunds ?? []);
      const remaining = payment.amount - alreadyRefunded;
      const amount = input.amount ?? remaining;

      if (amount <= 0) {
        logger.warn({ userId, paymentId, alreadyRefunded, failureReason: 'nothing_to_refund' }, 'Refund rejected: nothing left to refund');
        throw new ValidationError('Nothing left to refund');
      }
      if (amount > remaining) {
        logger.warn({ userId, paymentId, requested: amount, remaining, failureReason: 'refund_exceeds_balance' }, 'Refund rejected: exceeds refundable amount');
        throw new ValidationError(`Refund exceeds the refundable amount of ${remaining}`);
      }

      const providerRefund = await retryOperation(
        () =>
          this.provider.refund({
            providerRef: payment.providerRef as string,
            amount,
            currency: payment.currency,
            reason: input.reason,
            idempotencyKey,
          }),
        { ...this.retry, shouldRetry: (error) => isRetryableProviderError(error) }
      );

      const totalRefunded = alreadyRefunded + (providerRefund.status === 'succeeded' ? amount : 0);
      const nextStatus: PaymentStatus = totalRefunded >= payment.amount ? 'refunded' : 'partially_refunded';
      const shouldUpdateStatus = providerRefund.status === 'succeeded' && canTransition(payment.status as PaymentStatus, nextStatus);

      // Record the refund, optional status transition, and audit event atomically.
      const refund = await this.transactions.recordRefund(
        {
          paymentId: payment.id,
          idempotencyKey,
          amount,
          currency: payment.currency,
          reason: input.reason ?? null,
          status: providerRefund.status,
          providerRef: providerRefund.providerRef,
        },
        shouldUpdateStatus ? nextStatus : undefined,
        {
          type: PAYMENT_EVENT_TYPES.REFUND_CREATED,
          actorId: userId,
          payload: {
            refundId: undefined,
            amount,
            currency: payment.currency,
            reason: input.reason ?? null,
          },
        }
      );

      logger.info({ paymentId: payment.id, refundId: refund.id, amount }, 'Payment refunded');

      return this.toRefundResponse(refund);
    });
  }

  /**
   * Verifies and processes a provider webhook. Signature verification happens
   * before any state change and duplicate deliveries are ignored via the unique
   * provider event id.
   */
  async handleWebhook(providerName: string, payload: unknown, signature: string): Promise<WebhookResult> {
    return this.executeWithLogging('payments.webhook', async () => {
      if (providerName !== this.provider.name) {
        throw new ValidationError(`Unsupported payment provider "${providerName}"`);
      }

      const secret = this.webhookSecret;
      if (!secret) {
        throw new AppError('Webhook secret is not configured', 500, 'WEBHOOK_SECRET_MISSING');
      }

      const verification = verifyWebhookSignature({
        payload,
        signature,
        secret,
        toleranceSeconds: this.webhookToleranceSeconds,
      });
      if (!verification.valid) {
        logger.warn({ providerName, reason: verification.reason, failureReason: 'invalid_webhook_signature' }, 'Webhook rejected: invalid signature');
        throw new UnauthorizedError('Invalid webhook signature');
      }

      const event = this.parseWebhookEvent(payload);
      const existing = await this.prisma.paymentEvent.findFirst({
        where: { providerEventId: event.eventId },
      });
      if (existing) {
        return { received: true, duplicate: true, paymentId: existing.paymentId };
      }

      const payment = await this.prisma.payment.findFirst({
        where: { providerRef: event.providerRef, provider: this.provider.name },
      });
      if (!payment) {
        throw new PaymentNotFoundError('Payment for webhook event not found');
      }

      const transition = transitionForWebhookEvent(payment.status as PaymentStatus, event.type);
      const nextStatus = transition && canTransition(payment.status as PaymentStatus, transition) ? transition : undefined;

      await this.transactions.recordWebhookEvent(
        payment.id,
        nextStatus,
        {
          type: event.type,
          actorId: null,
          payload: {
            providerEventId: event.eventId,
            providerRef: event.providerRef,
            previousStatus: payment.status,
            nextStatus: nextStatus ?? payment.status,
          },
        }
      );

      return { received: true, duplicate: false, paymentId: payment.id };
    });
  }

  private parseWebhookEvent(payload: unknown): { eventId: string; providerRef: string; type: string } {
    if (!payload || typeof payload !== 'object') {
      throw new ValidationError('Webhook payload must be an object');
    }
    const record = payload as Record<string, unknown>;
    const eventId = record.id;
    const providerRef = record.providerRef ?? record.paymentIntentId;
    const type = record.type;
    if (typeof eventId !== 'string' || typeof providerRef !== 'string' || typeof type !== 'string') {
      throw new ValidationError('Webhook payload is missing required fields');
    }
    return { eventId, providerRef: providerRef as string, type: type as string };
  }

  private sumRefunded(refunds: Array<{ amount: number; status: string }>): number {
    return refunds
      .filter((zefund) => refund.status === 'succeeded')
      .reduce((sum, refund) => sum + refund.amount, 0);
  }

  private toResponse(payment: Prisma.PaymentGetPayload<{ refunds: true }>): PaymentResponse {
    return {
      id: payment.id,
      userId: payment.userId,
      creatorId: payment.creatorId,
      provider: payment.provider,
      providerRef: payment.providerRef,
      method: payment.method,
      amount: payment.amount,
      currency: payment.currency,
      status: payment.status as PaymentStatus,
      description: payment.description,
      refunds: (payment.refunds ?? []).map((refund) => this.toRefundResponse(refund)),
      createdAt: payment.createdAt,
      updatedAt: payment.updatedAt,
    };
  }

  private toRefundResponse(refund: Prisma.RefundGetPayload): RefundResponse {
    return {
      id: refund.id,
      paymentId: refund.paymentId,
      amount: refund.amount,
      currency: refund.currency,
      reason: refund.reason,
      status: refund.status,
      providerRef: refund.providerRef,
      createdAt: refund.createdAt,
    };
  }
}
