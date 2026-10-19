import { PrismaClient, Tip } from '@prisma/client';
import { BaseService } from '../../services/base.service';
import {
  CreateTipRequest,
  UpdateTipStatusRequest,
  TipResponse,
  TipStatus,
  BuildTransactionResponse,
  SubmitTransactionResponse,
} from './payment.types';
import {
  ValidationError,
  NotFoundError,
  UnauthorizedError,
  ConflictError,
  InvalidCurrencyError,
  PaymentDeclinedError,
  PaymentNotFoundError,
  ServiceUnavailableError,
} from '../../utils/errors';
import { buildPaymentTransaction, submitSignedTransaction } from '../../lib/stellar/transactions';
import { logger } from '../../utils/logger';
import { transactionRetryCounter } from '../../lib/metrics';
import { sanitizePageSize, sanitizePageNumber, parseSortParameters } from '../../utils/pagination';
import { paginateWithCursor } from '../../db/pagination';
import { WebhookService } from '../webhooks/webhook.service';
import { buildTipMemo, validateMemo, validatePaymentAmount } from '../../lib/stellar/validation';
import { checkMinimumTipAmount, type TipCurrency } from '../../config/payment-limits';
import { config } from '../../config/env';
import { MAX_MEDIA_PER_TIP, type MediaView } from '../media/media.types';
import { buildCdnUrl } from '../media/media.storage';
import { TIP_VISIBLE_STATE } from '../moderation/moderation.types';
import { TipFilterInput, buildTipWhere, describeTipFilters } from './tip-filters';
import { formatInTimezone } from '../../utils/timezone';
import { invalidateCaches, tipCacheKeys } from '../../lib/cache/invalidation';
import { withJobTracePayload } from '../../lib/requestContext';
import { validateWhitelistedAsset } from './payment.schemas';
import { TransactionService } from '../../services/transaction.service';
/**

 * Columns required to build a TipResponse.
 * Media columns selected alongside a tip. Only `ready` media is exposed: rows
 * still scanning, rejected, or failed are invisible to tip readers.
 */
const TIP_MEDIA_SELECT = {
  id: true,
  kind: true,
  status: true,
  mimeType: true,
  fileName: true,
  sizeBytes: true,
  width: true,
  height: true,
  durationSeconds: true,
  storageKey: true,
  derivatives: true,
  processingStatus: true,
  processingError: true,
  tipId: true,
  attachedAt: true,
  createdAt: true,
} as const;

/**
 * Columns required to build a `TipResponse`. Selecting explicitly keeps list
 * and lookup reads from dragging along columns the API never serializes.
 */
const TIP_RESPONSE_SELECT = {
  id: true,
  fromUserId: true,
  creatorId: true,
  amount: true,
  message: true,
  status: true,
  transactionHash: true,
  createdAt: true,
  updatedAt: true,
  media: {
    where: { status: 'ready' },
    orderBy: { attachedAt: 'asc' },
    select: TIP_MEDIA_SELECT,
  },

  assetCode: true,
  assetIssuer: true,
  assetDecimals: true,
  // Needed so a tip removed by a moderation decision 404s on direct lookup (#62).
  moderationState: true,
} as const;
function isUniqueConstraintError(error: unknown): boolean {
  const code = (error as { code?: string })?.code;
  return code === 'P2002';
}

/**
 * Content moderation read-side (#62): a tip that is hidden while a report is
 * open, or removed by a resolved decision, is not part of any public tip list.
 */
const VISIBLE_TIPS_ONLY = { moderationState: TIP_VISIBLE_STATE } as const;

class OptimisticLockError extends Error {
  constructor(public readonly tipId: string) {
    super(`Optimistic lock conflict for tip ${tipId}`);
    this.name = 'OptimisticLockError';
  }
}

/**
 * Payment state mutations are routed through `TransactionService` so related
 * writes (tip status + creator earnings, tip + media attachment) commit
 * atomically and never nest `$transaction` calls across service layers.
 */
export class PaymentService extends BaseService {
  private readonly transactions: TransactionService;

  constructor(private prisma: PrismaClient) {
    super();
    this.transactions = new TransactionService(prisma);
  }

  /**
   * Create a new tip (initial step before payment transaction)
   * Validates:
   * - Amount is positive
   * - Creator exists and is public and verified
   * - Sender has a verified wallet
   */
  async createTip(userId: string, data: CreateTipRequest): Promise<TipResponse & { jobId?: string }> {
    return this.executeWithLogging('payment.createTip', async () => {
      // Validate amount at the service boundary as well, so callers that bypass
      // the route layer (jobs, internal tooling) cannot create bad records.
      const amountCheck = validatePaymentAmount(data.amount);
      if (!amountCheck.valid) {
        throw new ValidationError(amountCheck.reason || 'Amount must be greater than 0');
      }

      // #218 — Reject 0-value/dust tips below the per-currency minimum. Repeats
      // the schema-level check so a caller that bypasses the route layer cannot
      // create a sub-minimum tip either. The minimum and currency are included
      // in the error so the caller knows exactly how much more is required.
      const minimumCheck = checkMinimumTipAmount(data.amount, data.currency as TipCurrency | undefined);
      if (!minimumCheck.valid) {
        throw new ValidationError(
          `Amount must be at least ${minimumCheck.minimum} ${minimumCheck.currency} (minimum tip amount)`,
          { minimumAmount: minimumCheck.minimum, currency: minimumCheck.currency }
        );
      }

      // Verify creator exists, is public, and is verified
      const creator = await this.prisma.creator.findUnique({
        where: { id: data.creatorId },
        select: { id: true, userId: true, isPublic: true, verified: true },
      });

      if (!creator) {
        throw new NotFoundError('Creator');
      }

      if (!creator.isPublic) {
        throw new ValidationError('Creator is not available for tips');
      }

      if (!creator.verified) {
        throw new ValidationError('Creator account must be verified to receive tips');
      }

      if (data.assetCode || data.assetIssuer) {
        await validateWhitelistedAsset(data.assetCode || 'USDC', data.assetIssuer, this.prisma);
      }

      const requestedAsset = data.assetId
        ? await this.prisma.stellarAsset.findFirst({ where: { id: data.assetId, enabled: true } })
        : await this.prisma.stellarAsset.findFirst({
            where: { enabled: true, code: 'USDC' },
            orderBy: { priority: 'desc' },
          });
      if (data.assetId && !requestedAsset) {
        logger.warn({ userId, assetId: data.assetId, failureReason: 'asset_unavailable' }, 'Tip blocked: requested asset is unavailable');
        throw new InvalidCurrencyError(data.assetId ?? 'unknown', { assetId: data.assetId });
      }

      // Verify sender is not tipping themselves
      const sender = await this.prisma.user.findUnique({
        where: { id: userId },
        select: { id: true },
      });

      if (!sender) {
        throw new NotFoundError('User');
      }

      if (creator.userId === userId) {
        throw new ValidationError('Cannot tip yourself');
      }

      // Verify sender wallet is verified
      const wallet = await this.prisma.wallet.findFirst({
        where: {
          userId,
          verified: true,
        },
        select: { id: true, publicKey: true },
      });

      if (!wallet) {
        throw new ValidationError('User does not have a verified wallet');
      }

      // #36 - Block payments from flagged wallets (medium/high/critical severity)
      const walletFlag = await this.prisma.walletFlag.findFirst({
        where: {
          address: wallet.publicKey,
          resolved: false,
          severity: { in: ['medium', 'high', 'critical'] },
        },
        select: { id: true },
      });

      if (walletFlag) {
        logger.warn({ userId, walletPublicKey: wallet.publicKey, failureReason: 'wallet_flagged' }, 'Tip blocked: sender wallet is flagged');
        throw new PaymentDeclinedError('Payment cannot be processed at this time', { reason: 'wallet_flagged' });
      }

      // #36 - Block payments to frozen creator accounts
      const now = new Date();
      const accountFreeze = await this.prisma.accountFreeze.findFirst({
        where: {
          creatorId: creator.id,
          resolved: false,
          OR: [{ expiresAt: null }, { expiresAt: { gt: now } }],
        },
        select: { id: true },
      });

      if (accountFreeze) {
        logger.warn({ userId, creatorId: data.creatorId, failureReason: 'account_frozen' }, 'Tip blocked: creator account is frozen');
        throw new PaymentDeclinedError('Creator is not available for tips at this time', { reason: 'account_frozen' });
      }

      // Idempotency: a retried submission that reuses its key must return the
      // tip created by the first attempt instead of creating a second charge.
      // This check runs BEFORE the Stellar balance call so retries short-circuit
      // without re-hitting the network. (#175 ordering fix)
      if (data.idempotencyKey) {
        const existing = await this.prisma.tip.findUnique({
          where: { idempotencyKey: data.idempotencyKey },
        });
        if (existing) {
          if (existing.fromUserId !== userId) {
            throw new ConflictError('Idempotency key already used by another user');
          }
          logger.info(`Idempotent tip creation replayed for key ${data.idempotencyKey}`);
          return this.formatTipResponse(existing);
        }
      }

      // #221 — Tip creation is asynchronous: the on-chain balance check (a
      // Stellar Horizon RPC call that can be slow or time out under network
      // congestion) is no longer made on this request path. The tip is
      // created immediately in PENDING state below, and a queued job
      // (`balanceVerificationQueue`, handled by
      // `lib/workers/balance-verification.worker.ts`) verifies the balance
      // afterwards, transitioning the tip to FAILED if funds are insufficient
      // or the check cannot be completed. `assetCode` is still resolved here
      // (not a network call) so it can be persisted on the tip and forwarded
      // to the job.
      const assetCode = requestedAsset?.code ?? 'USDC';

      // Validate requested media (#64) before creating the tip: every id must be
      // media this user uploaded, finished scanning, and has not already attached.
      // Resolving them up front means a tip never half-attaches and a foreign or
      // still-processing id fails the request instead of silently disappearing.
      const mediaIds = data.mediaIds ?? [];
      if (mediaIds.length > MAX_MEDIA_PER_TIP) {
        throw new ValidationError(`A tip can carry at most ${MAX_MEDIA_PER_TIP} media items`);
      }

      const attachableMedia = mediaIds.length
        ? await this.resolveAttachableMedia(userId, mediaIds)
        : [];

      if (attachableMedia.length !== mediaIds.length) {
        throw new ValidationError('One or more media items are not ready to be attached');
      }

      // Create tip in pending state
      let tip;
      try {
        tip = await this.transactions.createTip({
          data: {
            fromUserId: userId,
            creatorId: data.creatorId,
            amount: data.amount,
            assetId: requestedAsset?.id ?? null,
            assetCode: requestedAsset?.code ?? 'USDC',
            assetIssuer: requestedAsset?.issuer ?? null,
            assetDecimals: requestedAsset?.decimals ?? 7,
            message: data.message || null,
            status: TipStatus.PENDING,
            idempotencyKey: data.idempotencyKey ?? null,
            ...(attachableMedia.length
              ? { mediaIds: attachableMedia.map((media) => media.id) }
              : {}),
          },
        });
      } catch (error) {
        // Lost a concurrent create race on the same key: replay the winner.
        if (data.idempotencyKey && isUniqueConstraintError(error)) {
          const existing = await this.prisma.tip.findUnique({
            where: { idempotencyKey: data.idempotencyKey },
          });
          if (existing) {
            if (existing.fromUserId !== userId) {
              throw new ConflictError('Idempotency key already used by another user');
            }
            logger.info(`Idempotent tip creation replayed for key ${data.idempotencyKey}`);
            return this.formatTipResponse(existing);
          }
        }
        throw error;
      }

      logger.info(`Tip created: ${tip.id} from ${userId} to ${data.creatorId} for ${data.amount}`);
      await new WebhookService(this.prisma).dispatchEvent(tip.creatorId, tip.id, 'tip.created', {
        tipId: tip.id,
        amount: tip.amount,
        message: tip.message,
        status: tip.status,
      });

      if (attachableMedia.length) {
        // Stamp `attachedAt` so the media is ordered and the prune sweep can tell
        // an attached upload from an abandoned one. Best-effort: the tip exists
        // and is authoritative even if this bookkeeping write fails.
        await this.stampMediaAttached(
          tip.id,
          attachableMedia.map((media) => media.id)
        );
        logger.info(`Tip ${tip.id} attached ${attachableMedia.length} media item(s)`);
      }

      // #221 — Enqueue the balance verification job now that the tip exists.
      // Imported lazily, same reasoning as `checkTransactionConfirmation`
      // below: importing the queue module opens a Redis connection as a side
      // effect, which unit tests that import the service must not trigger.
      let jobId: string | undefined;
      try {
        const { balanceVerificationQueue } = await import('../../lib/queue');
        const job = await balanceVerificationQueue.add(
          'verify-balance',
          withJobTracePayload(
            {
              tipId: tip.id,
              walletPublicKey: wallet.publicKey,
              assetCode,
              requiredAmount: tip.amount,
            },
            { userId }
          ),
          {
            attempts: 5,
            backoff: { type: 'exponential', delay: 2000 },
          }
        );
        jobId = String(job.id);
      } catch (error) {
        // The tip already exists and is authoritative; a failure to enqueue
        // verification is logged, not surfaced, so a Redis hiccup never turns
        // into a failed tip creation request (the exact failure mode #221
        // exists to remove). The tip is left PENDING; an operational sweep or
        // manual retry can re-enqueue verification for it later.
        logger.error(
          { tipId: tip.id, error },
          'Failed to enqueue balance verification job; tip remains PENDING unverified'
        );
      }

      return { ...this.formatTipResponse(tip), jobId };
    });
  }

  /**
   * Load the subset of `mediaIds` that this user may attach to a tip right now:
   * owned by the user, verified (`ready`), and not already attached to a tip.
   * Returns the found rows so the caller can compare counts and detect misses.
   */
  private async resolveAttachableMedia(
    userId: string,
    mediaIds: string[]
  ): Promise<Array<{ id: string }>> {
    const unique = Array.from(new Set(mediaIds));
    const tipMedia = (this.prisma as unknown as { tipMedia?: any }).tipMedia;
    if (!tipMedia?.findMany) return [];

    const rows = await tipMedia.findMany({
      where: { id: { in: unique }, userId, status: 'ready', tipId: null },
      select: { id: true },
    });

    return Array.isArray(rows) ? rows : [];
  }

  /**
   * Record when media became part of a tip. Best-effort bookkeeping; failures are
   * logged rather than surfaced because the tip itself is already committed.
   */
  private async stampMediaAttached(tipId: string, mediaIds: string[]): Promise<void> {
    const tipMedia = (this.prisma as unknown as { tipMedia?: any }).tipMedia;
    if (!tipMedia?.updateMany) return;

    try {
      await this.transactions.stampMediaAttached({
        where: { id: { in: mediaIds }, tipId },
        data: { attachedAt: new Date() },
      });
    } catch (error) {
      logger.warn(`Failed to stamp attachedAt for tip ${tipId}: ${(error as Error).message}`);
    }
  }

  /**
   * Get a specific tip by ID
   */
  async getTip(tipId: string): Promise<TipResponse> {
    return this.executeWithLogging('payment.getTip', async () => {
      const tip = await this.prisma.tip.findUnique({
        where: { id: tipId },
        select: TIP_RESPONSE_SELECT,
      });

      if (!tip) {
        logger.warn({ tipId, failureReason: 'tip_not_found' }, 'Tip not found');
        throw new PaymentNotFoundError();
      }

      // Removed content is gone rather than hidden: a direct lookup 404s, while a
      // hidden tip stays readable so its sender and creator can see it (#62).
      if (tip.moderationState === 'removed') {
        logger.warn({ tipId, moderationState: tip.moderationState, failureReason: 'tip_removed' }, 'Tip access denied due to moderation removal');
        throw new PaymentNotFoundError();
      }

      return this.formatTipResponse(tip);
    });
  }

  /**
   * List tips received by a creator with offset pagination, multi-column sorting, and filtering
   */
  async listTips(
    creatorId: string,
    page: number = 1,
    pageSize: number = 20,
    options: TipFilterInput & {
      sortBy?: string;
      sortOrder?: 'asc' | 'desc';
    } = {}
  ): Promise<{
    tips: TipResponse[];
    total: number;
    page: number;
    pageSize: number;
    totalPages: number;
    hasNext: boolean;
    hasPrev: boolean;
    filters: Record<string, unknown>;
  }> {
    return this.executeWithLogging('payment.listTips', async () => {
      // Verify creator exists
      const creator = await this.prisma.creator.findUnique({
        where: { id: creatorId },
      });

      if (!creator) {
        throw new NotFoundError('Creator');
      }

      // Validate pagination
      const safePage = sanitizePageNumber(page);
      const safePageSize = sanitizePageSize(pageSize, 20);

      // Database-side filtering: public lists exclude tips hidden or removed
      // by moderation while retaining the existing tip filters.
      const where = buildTipWhere({ creatorId, ...VISIBLE_TIPS_ONLY }, options);

      const sortFields = parseSortParameters(
        options.sortBy,
        options.sortOrder,
        ['createdAt', 'amount', 'status', 'id', 'updatedAt'],
        'createdAt',
        'desc'
      );

      const orderBy = sortFields.map((s) => ({ [s.field]: s.direction }));
      const skip = (safePage - 1) * safePageSize;

      const [tips, total] = await Promise.all([
        this.prisma.tip.findMany({
          where,
          select: TIP_RESPONSE_SELECT,
          skip,
          take: safePageSize,
          orderBy,
        }),
        this.prisma.tip.count({ where }),
      ]);

      const totalPages = Math.ceil(total / safePageSize);

      return {
        tips: tips.map((tip) => this.formatTipResponse(tip, options.timezone)),
        total,
        page: safePage,
        pageSize: safePageSize,
        totalPages,
        hasNext: safePage < totalPages,
        hasPrev: safePage > 1,
        filters: describeTipFilters(options),
      };
    });
  }

  /**
   * List tips received by a creator with cursor-based pagination
   */
  async listTipsCursor(
    creatorId: string,
    params: TipFilterInput & {
      limit?: number;
      cursor?: string;
      after?: string;
      sortBy?: string;
      sortOrder?: 'asc' | 'desc';
    } = {}
  ) {
    return this.executeWithLogging('payment.listTipsCursor', async () => {
      const creator = await this.prisma.creator.findUnique({
        where: { id: creatorId },
      });

      if (!creator) {
        throw new NotFoundError('Creator');
      }

      const where = buildTipWhere({ creatorId, ...VISIBLE_TIPS_ONLY }, params);

      const result = await paginateWithCursor<Tip>(
        this.prisma.tip,
        {
          limit: params.limit,
          after: params.after || params.cursor,
          sortBy: params.sortBy,
          sortOrder: params.sortOrder,
        },
        {
          where,
          select: TIP_RESPONSE_SELECT,
          allowedSortFields: ['createdAt', 'amount', 'status', 'id', 'updatedAt'],
          defaultSortField: 'createdAt',
          defaultSortDirection: 'desc',
        }
      );

      return {
        ...result,
        items: result.items.map((tip) => this.formatTipResponse(tip, params.timezone)),
        data: result.data.map((tip) => this.formatTipResponse(tip, params.timezone)),
        filters: describeTipFilters(params),
      };
    });
  }

  /**
   * Get tip history for a user (tips they sent) with offset pagination, sorting, and filtering
   */
  async getUserTipHistory(
    userId: string,
    page: number = 1,
    pageSize: number = 20,
    options: TipFilterInput & {
      sortBy?: string;
      sortOrder?: 'asc' | 'desc';
    } = {}
  ): Promise<{
    tips: TipResponse[];
    total: number;
    page: number;
    pageSize: number;
    totalPages: number;
    hasNext: boolean;
    hasPrev: boolean;
    filters: Record<string, unknown>;
  }> {
    return this.executeWithLogging('payment.getUserTipHistory', async () => {
      // Validate pagination
      const safePage = sanitizePageNumber(page);
      const safePageSize = sanitizePageSize(pageSize, 20);

      const where = buildTipWhere({ fromUserId: userId, ...VISIBLE_TIPS_ONLY }, options);

      const sortFields = parseSortParameters(
        options.sortBy,
        options.sortOrder,
        ['createdAt', 'amount', 'status', 'id', 'updatedAt'],
        'createdAt',
        'desc'
      );

      const orderBy = sortFields.map((s) => ({ [s.field]: s.direction }));
      const skip = (safePage - 1) * safePageSize;

      const [tips, total] = await Promise.all([
        this.prisma.tip.findMany({
          where,
          select: TIP_RESPONSE_SELECT,
          skip,
          take: safePageSize,
          orderBy,
        }),
        this.prisma.tip.count({ where }),
      ]);

      const totalPages = Math.ceil(total / safePageSize);

      return {
        tips: tips.map((tip) => this.formatTipResponse(tip, options.timezone)),
        total,
        page: safePage,
        pageSize: safePageSize,
        totalPages,
        hasNext: safePage < totalPages,
        hasPrev: safePage > 1,
        filters: describeTipFilters(options),
      };
    });
  }

  /**
   * Get tip history for a user with cursor pagination
   */
  async getUserTipHistoryCursor(
    userId: string,
    params: TipFilterInput & {
      limit?: number;
      cursor?: string;
      after?: string;
      sortBy?: string;
      sortOrder?: 'asc' | 'desc';
    } = {}
  ) {
    return this.executeWithLogging('payment.getUserTipHistoryCursor', async () => {
      const where = buildTipWhere({ fromUserId: userId, ...VISIBLE_TIPS_ONLY }, params);

      const result = await paginateWithCursor<Tip>(
        this.prisma.tip,
        {
          limit: params.limit,
          after: params.after || params.cursor,
          sortBy: params.sortBy,
          sortOrder: params.sortOrder,
        },
        {
          where,
          select: TIP_RESPONSE_SELECT,
          allowedSortFields: ['createdAt', 'amount', 'status', 'id', 'updatedAt'],
          defaultSortField: 'createdAt',
          defaultSortDirection: 'desc',
        }
      );

      return {
        ...result,
        items: result.items.map((tip) => this.formatTipResponse(tip, params.timezone)),
        data: result.data.map((tip) => this.formatTipResponse(tip, params.timezone)),
        filters: describeTipFilters(params),
      };
    });
  }

  /**
   * Cross-creator tip search for analytics and moderation (#56).
   *
   * Unlike the history and creator listings there is no implicit scope, so the
   * caller supplies the filters explicitly (every one of them optional) and the
   * result carries the applied filters back plus the total matching count, so a
   * client can paginate without a second request. The route that exposes this
   * is admin-only because it can read tips the caller does not own.
   */
  async searchTips(
    page: number = 1,
    pageSize: number = 20,
    options: TipFilterInput & {
      sortBy?: string;
      sortOrder?: 'asc' | 'desc';
    } = {}
  ): Promise<{
    tips: TipResponse[];
    total: number;
    page: number;
    pageSize: number;
    totalPages: number;
    hasNext: boolean;
    hasPrev: boolean;
    filters: Record<string, unknown>;
  }> {
    return this.executeWithLogging('payment.searchTips', async () => {
      const safePage = sanitizePageNumber(page);
      const safePageSize = sanitizePageSize(pageSize, 20);

      const where = buildTipWhere({}, options);

      const sortFields = parseSortParameters(
        options.sortBy,
        options.sortOrder,
        ['createdAt', 'amount', 'status', 'id', 'updatedAt'],
        'createdAt',
        'desc'
      );

      const orderBy = sortFields.map((s) => ({ [s.field]: s.direction }));
      const skip = (safePage - 1) * safePageSize;

      const [tips, total] = await Promise.all([
        this.prisma.tip.findMany({
          where,
          select: TIP_RESPONSE_SELECT,
          skip,
          take: safePageSize,
          orderBy,
        }),
        this.prisma.tip.count({ where }),
      ]);

      const totalPages = Math.ceil(total / safePageSize);

      return {
        tips: tips.map((tip) => this.formatTipResponse(tip, options.timezone)),
        total,
        page: safePage,
        pageSize: safePageSize,
        totalPages,
        hasNext: safePage < totalPages,
        hasPrev: safePage > 1,
        filters: describeTipFilters(options),
      };
    });
  }

  /**
   * List tips for a creator using cursor-based keyset pagination.
   *
   * Distinct from the `listTipsCursor` above, which is the offset/keyset hybrid
   * consumed by the HTTP routes; this is the GraphQL-style connection shape.
   */
  async listTipsConnection(
    creatorId: string,
    params: { first?: number; after?: string; last?: number; before?: string } = {}
  ): Promise<{
    edges: Array<{ node: TipResponse; cursor: string }>;
    pageInfo: {
      hasNextPage: boolean;
      hasPreviousPage: boolean;
      startCursor: string | null;
      endCursor: string | null;
      totalCount?: number;
    };
    total?: number;
  }> {
    return this.executeWithLogging('payment.listTipsCursor', async () => {
      const creator = await this.prisma.creator.findUnique({
        where: { id: creatorId },
      });

      if (!creator) {
        throw new NotFoundError('Creator');
      }

      const limit = params.first ?? params.last ?? 20;
      if (limit < 1 || limit > 100) {
        throw new ValidationError('Limit must be between 1 and 100');
      }

      let cursorObj: { id: string; createdAt: string } | undefined;
      if (params.after) {
        try {
          const json = Buffer.from(params.after, 'base64url').toString('utf8');
          cursorObj = JSON.parse(json);
        } catch {
          throw new ValidationError('Invalid pagination cursor');
        }
      }

      const tips = await this.prisma.tip.findMany({
        where: {
          creatorId,
          ...VISIBLE_TIPS_ONLY,
        },
        take: limit + 1,
        ...(cursorObj ? { cursor: { id: cursorObj.id }, skip: 1 } : {}),
        orderBy: [{ createdAt: 'desc' }, { id: 'desc' }],
      });

      const hasMore = tips.length > limit;
      const nodes = hasMore ? tips.slice(0, limit) : tips;

      const edges = nodes.map((tip) => ({
        node: this.formatTipResponse(tip),
        cursor: Buffer.from(
          JSON.stringify({ id: tip.id, createdAt: tip.createdAt.toISOString() }),
          'utf8'
        ).toString('base64url'),
      }));

      const startCursor = edges.length > 0 ? edges[0].cursor : null;
      const endCursor = edges.length > 0 ? edges[edges.length - 1].cursor : null;

      return {
        edges,
        pageInfo: {
          hasNextPage: hasMore,
          hasPreviousPage: Boolean(params.after),
          startCursor,
          endCursor,
        },
      };
    });
  }

  /**
   * List user tip history using cursor-based keyset pagination.
   *
   * Distinct from the `getUserTipHistoryCursor` above (offset/keyset hybrid used
   * by the HTTP routes); this is the GraphQL-style connection shape.
   */
  async getUserTipHistoryConnection(
    userId: string,
    params: { first?: number; after?: string; last?: number; before?: string } = {}
  ): Promise<{
    edges: Array<{ node: TipResponse; cursor: string }>;
    pageInfo: {
      hasNextPage: boolean;
      hasPreviousPage: boolean;
      startCursor: string | null;
      endCursor: string | null;
    };
  }> {
    return this.executeWithLogging('payment.getUserTipHistoryCursor', async () => {
      const limit = params.first ?? params.last ?? 20;
      if (limit < 1 || limit > 100) {
        throw new ValidationError('Limit must be between 1 and 100');
      }

      let cursorObj: { id: string; createdAt: string } | undefined;
      if (params.after) {
        try {
          const json = Buffer.from(params.after, 'base64url').toString('utf8');
          cursorObj = JSON.parse(json);
        } catch {
          throw new ValidationError('Invalid pagination cursor');
        }
      }

      const tips = await this.prisma.tip.findMany({
        where: {
          fromUserId: userId,
          ...VISIBLE_TIPS_ONLY,
        },
        take: limit + 1,
        ...(cursorObj ? { cursor: { id: cursorObj.id }, skip: 1 } : {}),
        orderBy: [{ createdAt: 'desc' }, { id: 'desc' }],
      });

      const hasMore = tips.length > limit;
      const nodes = hasMore ? tips.slice(0, limit) : tips;

      const edges = nodes.map((tip) => ({
        node: this.formatTipResponse(tip),
        cursor: Buffer.from(
          JSON.stringify({ id: tip.id, createdAt: tip.createdAt.toISOString() }),
          'utf8'
        ).toString('base64url'),
      }));

      return {
        edges,
        pageInfo: {
          hasNextPage: hasMore,
          hasPreviousPage: Boolean(params.after),
          startCursor: edges.length > 0 ? edges[0].cursor : null,
          endCursor: edges.length > 0 ? edges[edges.length - 1].cursor : null,
        },
      };
    });
  }

  /**
   * Update tip status (typically used by transaction listener/confirmation service)
   * Can only update to specific statuses based on current state
   */
  async updateTipStatus(tipId: string, data: UpdateTipStatusRequest): Promise<TipResponse> {
    return this.executeWithLogging('payment.updateTipStatus', async () => {
      const maxRetries = 3;
      let attempt = 0;
      let shouldDispatchWebhook = false;
      let finalTip: TipResponse | undefined;

      while (true) {
        try {
          finalTip = await this.transactions.updateTipStatus(tipId, data.status, async (tx) => {
            const tip = await tx.tip.findUnique({ where: { id: tipId } });
            if (!tip) throw new NotFoundError('Tip');

            // Already in the requested state: a concurrent worker got here first
            // (or the client retried). A duplicate *confirmation* is a conflict;
            // any other same-state call is an idempotent no-op.
            if (tip.status === data.status) {
              if (data.status === TipStatus.COMPLETED) {
                logger.warn(
                  { tipId, status: tip.status },
                  'Duplicate tip confirmation rejected (already completed)'
                );
                throw new ConflictError('Tip has already been confirmed', {
                  tipId,
                  status: tip.status,
                  hint: 'This tip was already processed; do not retry the confirmation.',
                });
              }
              shouldDispatchWebhook = false;
              return this.formatTipResponse(tip);
            }

            const validTransitions: Record<string, string[]> = {
              [TipStatus.PENDING]: [TipStatus.COMPLETED, TipStatus.FAILED, TipStatus.CANCELLED],
              [TipStatus.COMPLETED]: [],
              [TipStatus.FAILED]: [TipStatus.PENDING],
              [TipStatus.CANCELLED]: [],
            };
            if (!validTransitions[tip.status]?.includes(data.status)) {
              logger.warn(
                { tipId, from: tip.status, to: data.status },
                'Unexpected tip status transition rejected'
              );
              throw new ConflictError(
                `Cannot transition tip from ${tip.status} to ${data.status}`,
                { tipId, from: tip.status, to: data.status }
              );
            }

            // Optimistic lock: only the writer holding the version we just read
            // may commit this transition. The loser gets count 0 and retries
            // against fresh state, where it observes the winner's status.
            const currentVersion = (tip as { version?: number }).version ?? 0;
            const changed = await tx.tip.updateMany({
              where: { id: tipId, status: tip.status, version: currentVersion },
              data: { status: data.status, version: { increment: 1 } },
            });
            if (changed.count !== 1) {
              throw new OptimisticLockError(tipId);
            }

            // Earnings are credited strictly inside the winning transition, so a
            // single tip can increment creator earnings exactly once.
            if (data.status === TipStatus.COMPLETED) {
              await tx.creator.update({
                where: { id: tip.creatorId },
                data: {
                  totalEarnings: { increment: tip.amount },
                  pendingBalance: { increment: tip.amount },
                },
              });
              logger.info(
                `Tip completed and creator earnings updated: ${tipId}, amount: ${tip.amount}`
              );
              shouldDispatchWebhook = true;
            } else {
              shouldDispatchWebhook = false; // Reset in case of retry
            }

            return this.formatTipResponse({ ...tip, status: data.status });
          });
          break; // Exit retry loop on success
        } catch (error: unknown) {
          attempt++;
          const code = (error as { code?: string })?.code;
          const retryable =
            error instanceof OptimisticLockError || code === 'P2028' || code === 'P2034';

          if (retryable && attempt < maxRetries) {
            transactionRetryCounter.inc({
              operation: 'payment.updateTipStatus',
              reason: error instanceof OptimisticLockError ? 'optimistic_lock' : code === 'P2034' ? 'serialization_failure' : 'deadlock',
            });
            logger.warn(
              { tipId, attempt, maxRetries },
              'Concurrent tip update detected; retrying with fresh state'
            );
            await new Promise((resolve) => setTimeout(resolve, 50 * Math.pow(2, attempt)));
            continue;
          }

          if (error instanceof OptimisticLockError) {
            throw new ConflictError('Tip was updated concurrently; reload and retry', {
              tipId,
              hint: 'Fetch the tip again and retry the transition with the current state.',
            });
          }
          throw error;
        }
      }

      if (!finalTip) {
        throw new ConflictError('Tip update did not complete', { tipId });
      }

      // Process referral commission outside transaction (idempotent)
      if (shouldDispatchWebhook && finalTip) {
        try {
          // Look up the sender's userId from the tip (fromUserId is the referee)
          const tipRecord = await this.prisma.tip.findUnique({
            where: { id: finalTip.id },
            select: { fromUserId: true, amount: true },
          });
          if (tipRecord) {
            const { ReferralService } = await import('../referrals/referral.service');
            const referralService = new ReferralService(this.prisma);
            await referralService.processCommissionForTip({
              tipId: finalTip.id,
              tipAmount: tipRecord.amount,
              refereeUserId: tipRecord.fromUserId,
            });
          }
        } catch (e) {
          // Commission failures must never block a confirmed tip
          logger.error(`Failed to process referral commission for tip ${finalTip.id}:`, e);
        }
      }

      // Dispatch webhooks outside transaction
      if (shouldDispatchWebhook) {
        try {
          await new WebhookService(this.prisma).dispatchEvent(
            finalTip.creatorId,
            finalTip.id,
            'payment.completed',
            {
              tipId: finalTip.id,
              amount: finalTip.amount,
              transactionHash: finalTip.transactionHash,
            }
          );
        } catch (error) {
          logger.error({ error, tipId }, 'Failed to dispatch payment.completed webhook');
        }
      }

      return finalTip;
    });
  }

  /**
   * Build a Stellar payment transaction for frontend signing
   * Frontend will sign this transaction with user's wallet and submit it back
   */
  async buildPaymentTransaction(
    tipId: string,
    senderPublicKey: string,
    creatorPublicKey: string,
    amount: string,
    assetCode?: string,
    assetIssuer?: string,
    memo?: string
  ): Promise<BuildTransactionResponse> {
    return this.executeWithLogging('payment.buildTransaction', async () => {
      // Stellar text memos are capped at MEMO_MAX_LENGTH UTF-8 bytes (#180). The
      // route already answers 400 for an oversized memo; the check is repeated
      // here so callers that bypass the route layer also fail before any
      // database or network call. Without a caller memo the tip id is used.
      const transactionMemo = memo ?? buildTipMemo(tipId);
      const memoCheck = validateMemo(transactionMemo);
      if (!memoCheck.valid) {
        throw new ValidationError(memoCheck.reason || 'Invalid transaction memo', {
          field: 'memo',
        });
      }

      // Validate asset code and issuer against configured whitelisted assets (#185)
      await validateWhitelistedAsset(assetCode || 'USDC', assetIssuer, this.prisma);

      const tip = await this.prisma.tip.findUnique({
        where: { id: tipId },
        select: { id: true, fromUserId: true, status: true },
      });

      if (!tip) {
        throw new NotFoundError('Tip');
      }

      if (tip.status !== TipStatus.PENDING) {
        throw new ValidationError('Can only build transaction for pending tips');
      }

      // Verify sender wallet exists and matches tip sender
      const sender = await this.prisma.wallet.findFirst({
        where: {
          publicKey: senderPublicKey,
          verified: true,
        },
        select: { id: true, userId: true },
      });

      if (!sender || sender.userId !== tip.fromUserId) {
        throw new UnauthorizedError('Wallet does not match tip sender');
      }

      try {
        // Build transaction
        const transactionBuilder = await buildPaymentTransaction({
          senderPublicKey,
          recipientPublicKey: creatorPublicKey,
          amount,
          assetCode,
          assetIssuer,
          memo: transactionMemo,
        });

        const transaction = transactionBuilder;
        const transactionEnvelope = transaction.toXDR();

        logger.debug(`Payment transaction built for tip: ${tipId}`);

        return {
          transactionEnvelope: transactionEnvelope,
          tipId,
          fee: 100, // Base fee in stroops
        };
      } catch (error) {
        logger.error({ userId: tip.fromUserId, tipId, failureReason: 'build_failed', cause: (error as Error).message }, 'Failed to build payment transaction');
        throw new PaymentDeclinedError('Failed to build payment transaction', { tipId });
      }
    });
  }

  /**
   * Submit a signed payment transaction
   * Stores the transaction hash and updates tip status
   */
  async submitPaymentTransaction(
    tipId: string,
    transactionEnvelope: string
  ): Promise<SubmitTransactionResponse> {
    return this.executeWithLogging('payment.submitTransaction', async () => {
      const tip = await this.prisma.tip.findUnique({
        where: { id: tipId },
        select: { id: true, status: true, transactionHash: true },
      });

      if (!tip) {
        throw new NotFoundError('Tip');
      }

      // Idempotency: a tip that already carries a transaction hash was submitted
      // before — return the stored result instead of submitting a second time.
      if (tip.transactionHash) {
        logger.warn(
          { tipId, transactionHash: tip.transactionHash },
          'Duplicate payment submission detected; returning existing transaction'
        );
        return {
          tipId,
          transactionHash: tip.transactionHash,
          status: tip.status as TipResponse['status'],
        };
      }

      if (tip.status !== TipStatus.PENDING) {
        throw new ConflictError('Can only submit transaction for pending tips', {
          tipId,
          status: tip.status,
        });
      }

      // Submit transaction to Stellar network. This is the only non-transactional
      // step; the unique transactionHash constraint below makes persistence
      // idempotent if two submissions race.
      let result: Awaited<ReturnType<typeof submitSignedTransaction>>;
      try {
        result = await submitSignedTransaction(transactionEnvelope);
      } catch (error) {
        logger.error({ tipId, failureReason: 'submit_failed', cause: (error as Error).message }, 'Failed to submit transaction to Stellar network');
        throw new PaymentDeclinedError('Failed to submit payment transaction', { tipId });
      }

      try {
        const updatedTip = await this.transactions.recordTransactionHash({
          where: { id: tipId },
          data: {
            transactionHash: result.transactionHash,
          },
          select: { status: true },
        });

        logger.info(
          `Payment transaction submitted for tip: ${tipId}, hash: ${result.transactionHash}`
        );

        return {
          tipId,
          transactionHash: result.transactionHash,
          status: updatedTip.status as TipResponse['status'],
        };
      } catch (error) {
        // Unique constraint on transactionHash: a concurrent submission already
        // persisted this hash. Never overwrite it — replay the stored value.
        if (isUniqueConstraintError(error)) {
          const winner = await this.prisma.tip.findUnique({
            where: { id: tipId },
            select: { status: true, transactionHash: true },
          });
          if (winner?.transactionHash) {
            logger.warn(
              { tipId, transactionHash: winner.transactionHash },
              'Concurrent payment submission lost the race; returning stored transaction'
            );
            return {
              tipId,
              transactionHash: winner.transactionHash,
              status: winner.status as TipResponse['status'],
            };
          }
          throw new ConflictError('Transaction has already been submitted for this tip', { tipId });
        }
        logger.error({ tipId, failureReason: 'persist_failed', cause: (error as Error).message }, 'Failed to persist transaction hash');
        throw new PaymentDeclinedError('Failed to record payment transaction', { tipId });
      }
    });
  }

  /**
   * Check and update transaction confirmation status
   * Called to verify if a submitted transaction has been confirmed on the network
   */
  async checkTransactionConfirmation(tipId: string): Promise<TipResponse> {
    return this.executeWithLogging('payment.checkConfirmation', async () => {
      const tip = await this.prisma.tip.findUnique({
        where: { id: tipId },
        select: TIP_RESPONSE_SELECT,
      });

      if (!tip) {
        throw new NotFoundError('Tip');
      }

      if (!tip.transactionHash) {
        throw new ValidationError('No transaction hash found for this tip');
      }

      // Queue the Stellar confirmation check as async job instead of blocking.
      // Imported lazily so loading the payment service never opens a Redis
      // connection as a side effect (keeps unit tests network-free).
      const { stellarConfirmationQueue } = await import('../../lib/queue');
      await stellarConfirmationQueue.add(
        'confirm-transaction',
        withJobTracePayload(
          {
            transactionId: tipId,
            tipId,
            transactionHash: tip.transactionHash,
          },
          { userId: tip.fromUserId }
        ),
        {
          attempts: 60,
          backoff: { type: 'exponential', delay: 1000 },
        }
      );

      logger.info(`Queued Stellar confirmation check for tip ${tipId}`);

      // Return current tip status immediately (job runs in background)
      return this.formatTipResponse(tip);
    });
  }

  /**
   * Confirm a submitted transaction by polling the Stellar network and
   * transitioning the tip to its terminal state.
   *
   * This is the synchronous counterpart to `checkTransactionConfirmation`
   * (which only enqueues an async job). It is invoked by the confirmation
   * worker and by integration tests that exercise the full confirmation
   * workflow without a live Redis queue.
   *
   * Retry strategy (mirrors the queue's `attempts`/`backoff` config):
   *  - The worker polls up to `maxAttempts` times with exponential backoff.
   *  - A transaction that is still pending after the retry limit is left in
   *    PENDING (the worker gives up; a later sweep may re-enqueue it).
   *  - Transient network errors during polling are retried; a persistent
   *    error surfaces as `ServiceUnavailableError` so the job can be retried.
   *  - Confirmation is idempotent: re-running it against an already-completed
   *    tip is a no-op (no duplicate earnings, no duplicate webhook).
   */
  async confirmTransaction(
    tipId: string,
    options: {
      maxAttempts?: number;
      baseDelayMs?: number;
      poll?: (transactionHash: string) => Promise<'success' | 'pending' | 'failed'>;
    } = {}
  ): Promise<TipResponse> {
    return this.executeWithLogging('payment.confirmTransaction', async () => {
      const maxAttempts = options.maxAttempts ?? 3;
      const baseDelayMs = options.baseDelayMs ?? 10;

      const tip = await this.prisma.tip.findUnique({
        where: { id: tipId },
        select: { id: true, status: true, transactionHash: true },
      });

      if (!tip) {
        throw new NotFoundError('Tip');
      }

      if (!tip.transactionHash) {
        throw new ValidationError('No transaction hash found for this tip');
      }

      // Idempotency: a tip that already reached a terminal state is not
      // re-confirmed. Returning the current record keeps retries safe.
      if (tip.status === TipStatus.COMPLETED || tip.status === TipStatus.FAILED || tip.status === TipStatus.CANCELLED) {
        logger.info(
          { tipId, status: tip.status },
          'Confirmation skipped: tip already in terminal state'
        );
        return this.getTip(tipId);
      }

      const poll =
        options.poll ??
        (async (transactionHash: string) => {
          const { getTransactionStatus } = await import('../../lib/stellar/transactions');
          return getTransactionStatus(transactionHash);
        });

      let lastError: unknown;
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        let outcome: 'success' | 'pending' | 'failed';
        try {
          outcome = await poll(tip.transactionHash);
        } catch (error) {
          lastError = error;
          logger.warn(
            { tipId, attempt, maxAttempts, cause: (error as Error).message },
            'Stellar confirmation poll failed; retrying'
          );
          if (attempt < maxAttempts) {
            await new Promise((resolve) =>
              setTimeout(resolve, baseDelayMs * Math.pow(2, attempt - 1))
            );
            continue;
          }
          throw new ServiceUnavailableError(
            'Unable to confirm transaction on the Stellar network',
            { tipId, attempts: attempt }
          );
        }

        if (outcome === 'success') {
          return this.updateTipStatus(tipId, { status: TipStatus.COMPLETED });
        }

        if (outcome === 'failed') {
          return this.updateTipStatus(tipId, { status: TipStatus.FAILED });
        }

        // Still pending: back off and poll again.
        if (attempt < maxAttempts) {
          await new Promise((resolve) =>
            setTimeout(resolve, baseDelayMs * Math.pow(2, attempt - 1))
          );
        }
      }

      // Pending beyond the retry limit: leave the tip PENDING so a later sweep
      // can re-enqueue it. Do not mark it FAILED — the transaction may still
      // land on-chain.
      logger.warn(
        { tipId, attempts: maxAttempts, cause: (lastError as Error | undefined)?.message },
        'Transaction still pending after retry limit; leaving tip in PENDING'
      );
      return this.getTip(tipId);
    });
  }

  /**
   * Format database tip record to response DTO
   */
  private formatTipResponse(
    tip: {
      id: string;
      fromUserId: string;
      creatorId: string;
      amount: number;
      message: string | null;
      status: string;
      transactionHash?: string | null;
      createdAt: Date;
      updatedAt: Date;
      media?: any[];
      assetCode?: string | null;
      assetIssuer?: string | null;
      assetDecimals?: number | null;
    },
    timezone?: string
  ): TipResponse {
    return {
      id: tip.id,
      fromUserId: tip.fromUserId,
      creatorId: tip.creatorId,
      amount: tip.amount,
      message: tip.message,
      status: tip.status as TipResponse['status'],
      transactionHash: tip.transactionHash || null,
      media: Array.isArray(tip.media)
        ? tip.media.map((row: any) => this.formatAttachedMedia(row))
        : [],
      // Formatted in the caller's requested timezone when listing/filtering
      // by date range (issue #216); every other call site omits `timezone`
      // and gets the previous plain-UTC ISO string, unchanged.
      createdAt: formatInTimezone(tip.createdAt, timezone),
      updatedAt: formatInTimezone(tip.updatedAt, timezone),
      assetCode: tip.assetCode ?? 'USDC',
      assetIssuer: tip.assetIssuer ?? null,
      assetDecimals: tip.assetDecimals ?? 7,
    };
  }

  /**
   * Render a `TipMedia` row as a `MediaView`. Mirrors the media domain's view
   * builder so a tip read and a media read describe the same file identically.
   */
  private formatAttachedMedia(media: any): MediaView {
    const derivatives = Array.isArray(media.derivatives) ? media.derivatives : [];
    const derivativeUrl = (variant: string): string | null => {
      const derivative = derivatives.find((entry: any) => entry?.variant === variant);
      if (!derivative?.key) return null;
      return (
        buildCdnUrl(config.MEDIA_CDN_BASE_URL, derivative.key) ??
        `/api/v1/media/${media.id}/content?variant=${variant}`
      );
    };

    return {
      id: media.id,
      kind: media.kind,
      status: media.status,
      mimeType: media.mimeType,
      fileName: media.fileName,
      sizeBytes: media.sizeBytes ?? 0,
      width: media.width ?? null,
      height: media.height ?? null,
      durationSeconds: media.durationSeconds ?? null,
      url:
        buildCdnUrl(config.MEDIA_CDN_BASE_URL, media.storageKey) ??
        `/api/v1/media/${media.id}/content`,
      previewUrl: derivativeUrl('preview'),
      thumbnailUrl: derivativeUrl('thumbnail'),
      processing: {
        status: media.processingStatus ?? 'pending',
        error: media.processingError ?? null,
      },
      createdAt: (media.createdAt instanceof Date
        ? media.createdAt
        : new Date(media.createdAt)
      ).toISOString(),
      attachedTipId: media.tipId ?? null,
    };
  }
}
