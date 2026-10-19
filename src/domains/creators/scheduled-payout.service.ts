/*
 * Scheduled creator payouts (#222).
 *
 * A creator can configure a recurring payout (daily, weekly, monthly, or a
 * custom cron expression) instead of manually requesting one each time. The
 * recurrence itself is driven by BullMQ's job scheduler
 * (`Queue.upsertJobScheduler` / `removeJobScheduler`), which needs no
 * separately managed cron process: BullMQ evaluates the cron expression
 * against Redis and enqueues a run of the job on schedule on its own. This
 * service owns the durable `ScheduledPayment` row (what was configured, its
 * status, and its last-run bookkeeping) and keeps the BullMQ scheduler in
 * sync with it; the worker that actually executes a triggered payout lives in
 * `src/lib/workers/scheduled-payout.worker.ts`.
 *
 * Scope note (see PR body): the CRUD here (create/pause/resume/cancel/list)
 * and the scheduler registration calls are fully implemented, but end-to-end
 * "a cron fires and a real payout executes" has not been exercised against a
 * live Redis instance in this session; see the PR body's Disclosures section.
 */
import { PrismaClient, ScheduledPaymentStatus } from '@prisma/client';
import { BaseService } from '../../services/base.service';
import { TransactionService } from '../../services/transaction.service';
import { NotFoundError, ValidationError } from '../../utils/errors';
import { logger } from '../../utils/logger';
import { config } from '../../config/env';
import { scheduledPayoutQueue } from '../../lib/queue';
import { resolveCronExpression, validateCronExpression } from '../../lib/cron';
import type { CreateScheduledPayoutInput } from './scheduled-payout.schemas';

export interface ScheduledPayoutView {
  id: string;
  creatorId: string;
  amount: number;
  walletAddress: string | null;
  cronExpression: string;
  status: ScheduledPaymentStatus;
  lastRunAt: string | null;
  lastPayoutId: string | null;
  lastRunError: string | null;
  createdAt: string;
  updatedAt: string;
}

/** BullMQ job scheduler template: the job each scheduled run enqueues. */
const SCHEDULED_PAYOUT_JOB_NAME = 'execute-scheduled-payout';

export class ScheduledPayoutService extends BaseService {
  private readonly transactions: TransactionService;

  constructor(private prisma: PrismaClient) {
    super();
    this.transactions = new TransactionService(prisma);
  }

  /**
   * Creates a scheduled payout for a creator and registers its recurring job
   * with BullMQ. The minimum payout amount reuses `config.MIN_PAYOUT_AMOUNT`,
   * the same floor `PayoutService.processPayout` already enforces for manual
   * payouts, so a creator cannot schedule a recurring payout below the amount
   * a one-off payout would also reject.
   */
  async createScheduledPayout(
    creatorId: string,
    input: CreateScheduledPayoutInput
  ): Promise<ScheduledPayoutView> {
    return this.executeWithLogging('scheduledPayout.create', async () => {
      const minimumPayout = config.MIN_PAYOUT_AMOUNT || 50;
      if (input.amount < minimumPayout) {
        throw new ValidationError(`Scheduled payout amount must be at least ${minimumPayout} XLM`);
      }

      const cronExpression = resolveCronExpression(input);
      if (!cronExpression) {
        throw new ValidationError('Either recurrence or cronExpression is required');
      }

      const cronCheck = validateCronExpression(cronExpression);
      if (!cronCheck.valid) {
        throw new ValidationError(cronCheck.reason ?? 'Invalid cron expression');
      }

      const scheduled = await this.transactions.createScheduledPayout({
        creatorId,
        amount: input.amount,
        walletAddress: input.walletAddress ?? null,
        cronExpression,
        status: ScheduledPaymentStatus.active,
      });

      await this.registerJobScheduler(scheduled.id, cronExpression, creatorId);

      logger.info(
        { creatorId, scheduledPaymentId: scheduled.id, cronExpression },
        'Scheduled payout created'
      );

      return this.formatScheduledPayout(scheduled);
    });
  }

  /** Lists every scheduled payout for a creator, newest first. */
  async listScheduledPayouts(creatorId: string): Promise<ScheduledPayoutView[]> {
    return this.executeWithLogging('scheduledPayout.list', async () => {
      const rows = await this.prisma.scheduledPayment.findMany({
        where: { creatorId },
        orderBy: { createdAt: 'desc' },
      });
      return rows.map((row) => this.formatScheduledPayout(row));
    });
  }

  /** A single scheduled payout, scoped to the owning creator. */
  async getScheduledPayout(creatorId: string, id: string): Promise<ScheduledPayoutView> {
    return this.executeWithLogging('scheduledPayout.get', async () => {
      const row = await this.findOwned(creatorId, id);
      return this.formatScheduledPayout(row);
    });
  }

  /** Pauses a scheduled payout: deregisters the BullMQ scheduler, keeps the row. */
  async pauseScheduledPayout(creatorId: string, id: string): Promise<ScheduledPayoutView> {
    return this.executeWithLogging('scheduledPayout.pause', async () => {
      const row = await this.findOwned(creatorId, id);
      if (row.status === ScheduledPaymentStatus.cancelled) {
        throw new ValidationError('Cannot pause a cancelled scheduled payout');
      }

      if (row.status === ScheduledPaymentStatus.active) {
        await scheduledPayoutQueue.removeJobScheduler(row.id);
      }

      const updated = await this.transactions.updateScheduledPayoutStatus(
        row.id,
        ScheduledPaymentStatus.paused
      );

      logger.info({ creatorId, scheduledPaymentId: row.id }, 'Scheduled payout paused');
      return this.formatScheduledPayout(updated);
    });
  }

  /** Resumes a paused scheduled payout: re-registers the BullMQ scheduler. */
  async resumeScheduledPayout(creatorId: string, id: string): Promise<ScheduledPayoutView> {
    return this.executeWithLogging('scheduledPayout.resume', async () => {
      const row = await this.findOwned(creatorId, id);
      if (row.status === ScheduledPaymentStatus.cancelled) {
        throw new ValidationError('Cannot resume a cancelled scheduled payout');
      }
      if (row.status === ScheduledPaymentStatus.active) {
        return this.formatScheduledPayout(row);
      }

      await this.registerJobScheduler(row.id, row.cronExpression, creatorId);

      const updated = await this.transactions.updateScheduledPayoutStatus(
        row.id,
        ScheduledPaymentStatus.active
      );

      logger.info({ creatorId, scheduledPaymentId: row.id }, 'Scheduled payout resumed');
      return this.formatScheduledPayout(updated);
    });
  }

  /** Cancels a scheduled payout permanently: deregisters the scheduler. */
  async cancelScheduledPayout(creatorId: string, id: string): Promise<ScheduledPayoutView> {
    return this.executeWithLogging('scheduledPayout.cancel', async () => {
      const row = await this.findOwned(creatorId, id);
      if (row.status === ScheduledPaymentStatus.cancelled) {
        return this.formatScheduledPayout(row);
      }

      if (row.status === ScheduledPaymentStatus.active) {
        await scheduledPayoutQueue.removeJobScheduler(row.id);
      }

      const updated = await this.transactions.updateScheduledPayoutStatus(
        row.id,
        ScheduledPaymentStatus.cancelled
      );

      logger.info({ creatorId, scheduledPaymentId: row.id }, 'Scheduled payout cancelled');
      return this.formatScheduledPayout(updated);
    });
  }

  /** Records the outcome of a triggered run (called by the worker). */
  async recordRunOutcome(
    id: string,
    outcome: { payoutId?: string; error?: string }
  ): Promise<void> {
    return this.executeWithLogging('scheduledPayout.recordRunOutcome', async () => {
      await this.transactions.recordScheduledPayoutRunOutcome(id, {
        payoutId: outcome.payoutId,
        error: outcome.error,
      });
    });
  }

  private async findOwned(creatorId: string, id: string) {
    const row = await this.prisma.scheduledPayment.findUnique({ where: { id } });
    if (!row || row.creatorId !== creatorId) {
      throw new NotFoundError('Scheduled payout');
    }
    return row;
  }

  private async registerJobScheduler(
    scheduledPaymentId: string,
    cronExpression: string,
    creatorId: string
  ): Promise<void> {
    await scheduledPayoutQueue.upsertJobScheduler(
      scheduledPaymentId,
      { pattern: cronExpression },
      {
        name: SCHEDULED_PAYOUT_JOB_NAME,
        data: { scheduledPaymentId, creatorId },
      }
    );
  }

  private formatScheduledPayout(row: {
    id: string;
    creatorId: string;
    amount: number;
    walletAddress: string | null;
    cronExpression: string;
    status: ScheduledPaymentStatus;
    lastRunAt: Date | null;
    lastPayoutId: string | null;
    lastRunError: string | null;
    createdAt: Date;
    updatedAt: Date;
  }): ScheduledPayoutView {
    return {
      id: row.id,
      creatorId: row.creatorId,
      amount: row.amount,
      walletAddress: row.walletAddress,
      cronExpression: row.cronExpression,
      status: row.status,
      lastRunAt: row.lastRunAt ? row.lastRunAt.toISOString() : null,
      lastPayoutId: row.lastPayoutId,
      lastRunError: row.lastRunError,
      createdAt: row.createdAt.toISOString(),
      updatedAt: row.updatedAt.toISOString(),
    };
  }
}
