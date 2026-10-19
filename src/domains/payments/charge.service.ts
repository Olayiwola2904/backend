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

      const payment = await this.transactions.runInPaymentTransaction(async (tx) => {
        const created = await tx.payment.create({
          data: {
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
          include: { refunds: true },
        });

        await this.recordEventInTransaction(tx, created.id, PAYMENT_EVENT_TYPES.CREATED, userId, {
          provider: this.provider.name,
          providerRef: intent.providerRef,
          amount: input.amount,
          currency: input.currency,
          method: input.method,
        });

        return created;
      });

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

      const { refund } = await this.transactions.runInPaymentTransaction(async (tx) => {
        const createdRefund = await tx.refund.create({
          data: {
            paymentId: payment.id,
            idempotencyKey,
            amount,
            currency: payment.currency,
            reason: input.reason ?? null,
            status: providerRefund.status,
            providerRef: providerRefund.providerRef,
          },
        });

        const totalRefunded = alreadyRefunded + (providerRefund.status === 'succeeded' ? amount : 0);
        const nextStatus: PaymentStatus = totalRefunded >= payment.amount ? 'refunded' : 'partially_refunded';
        if (providerRefund.status === 'succeeded' && canTransition(payment.status as PaymentStatus, nextStatus)) {
          await tx.payment.update({ where: { id: payment.id }, data: { status: nextStatus } });
        }

        await this.recordEventInTransaction(tx, payment.id, PAYMENT_EVENT_TYPES.REFUND_CREATED, userId, {
          refundId: createdRefund.id,
          amount,
          currency: payment.currency,
          reason: input.reason ?? null,
        });

        return { refund: createdRefund };
      });

      logger.info({ paymentId: payment.id, refundId: refund.id, amount }, 'Payment refunded');

      return this.toRefundResponse(refund);
    });
  }

  /**
   * Verifies and processes a provider webhook. Signature verification and
   * idempotent status transitions are performed in a single transaction.
   */
  async handleWebhook(payload: string, signature: string): Promise<WebhookResult> {
    return this.executeWithLogging('payments.webhook', async () => {
      if (!this.webhookSecret) {
        throw new ValidationError('Webhook secret is not configured');
      }

      const verified = verifyWebhookSignature(payload, signature, this.webhookSecret, this.webhookToleranceSeconds);
      if (!verified) {
        throw new UnauthorizedError('Invalid webhook signature');
      }

      const event = JSON.parse(payload) as {
        id?: string;
        type?: string;
        data?: { providerRef?: string; paymentId?: string; amount?: number };
      };

      const providerRef = event.data?.providerRef;
      const paymentId = event.data?.paymentId;
      if (!providerRef && !paymentId) {
        throw new ValidationError('Webhook payload is missing a payment reference');
      }

      const payment = await this.prisma.payment.findFirst({
        where: paymentId ? { id: paymentId } : { providerRef },
        include: { refunds: true },
      });
      if (!payment) {
        throw new PaymentNotFoundError('Payment for webhook event not found');
      }

      const nextStatus = transitionForWebhookEvent(event.type ?? '');
      if (!nextStatus) {
        return { received: true, processed: false, paymentId: payment.id, status: payment.status as PaymentStatus };
      }

      if (payment.status === nextStatus) {
        return { received: true, processed: false, paymentId: payment.id, status: payment.status as PaymentStatus };
      }

      if (!canTransition(payment.status as PaymentStatus, nextStatus)) {
        logger.warn({ paymentId: payment.id, from: payment.status, to: nextStatus }, 'Webhook status transition rejected');
        throw new ValidationError(`Invalid payment status transition from "${payment.status}" to "${nextStatus}"`);
      }

      await this.transactions.runInPaymentTransaction(async (tx) => {
        await tx.payment.update({ where: { id: payment.id }, data: { status: nextStatus } });
        await this.recordEventInTransaction(tx, payment.id, PAYMENT_EVENT_TYPES.STATUS_CHANGED, payment.userId, {
          from: payment.status,
          to: nextStatus,
          eventId: event.id ?? null,
          eventType: event.type ?? null,
        });
      });

      return { received: true, processed: true, paymentId: payment.id, status: nextStatus };
    });
  }

  private async recordEventInTransaction(
    tx: Prisma.TransactionClient,
    paymentId: string,
    type: string,
    actorId: string,
    data: Record<string, unknown>
  ): Promise<void> {
    await tx.paymentEvent.create({
      data: {
        paymentId,
        type,
        actorId,
        data: data as Prisma.InputJsonObject,
      },
    });
  }

  private sumRefunded(refunds: Array<{ amount: number; status: string }>): number {
    return refunds
      .filter((refund) => refund.status === 'succeeded')
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
