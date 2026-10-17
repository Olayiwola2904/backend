import { PrismaClient, Tip } from '@prisma/client';
import { BaseService } from '../../services/base.service';
import { TransactionService } from '../../services/transaction.service';
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
  PaymentRequiredError,
  ServiceUnavailableError,
} from '../../utils/errors';
import { buildPaymentTransaction, submitSignedTransaction } from '../../lib/stellar/transactions';
import { getWalletBalance } from '../../lib/stellar/wallet';
import { logger } from '../../utils/logger';
import { sanitizePageSize, sanitizePageNumber, parseSortParameters } from '../../utils/pagination';
import { paginateWithCursor } from '../../db/pagination';
import { WebhookService } from '../webhooks/webhook.service';
import { buildTipMemo, validateMemo, validatePaymentAmount } from '../../lib/stellar/validation';
import { config } from '../../config/env';
import { MAX_MEDIA_PER_TIP, type MediaView } from '../media/media.types';
import { buildCdnUrl } from '../media/media.storage';
import { TIP_VISIBLE_STATE } from '../moderation/moderation.types';
import { TipFilterInput, buildTipWhere, describeTipFilters } from './tip-filters';
import { invalidateCaches, tipCacheKeys } from '../../lib/cache/invalidation';

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

export class PaymentService extends BaseService {
  private readonly transactionService: TransactionService;

  constructor(private prisma: PrismaClient) {
    super();
    this.transactionService = new TransactionService(prisma);
  }

  /**
   * Create a new tip (initial step before payment transaction)
   * Validates:
   * - Amount is positive
   * - Creator exists and is public and verified
   * - Sender has a verified wallet
   */
  async createTip(userId: string, data: CreateTipRequest): Promise<TipResponse> {
    return this.executeWithLogging('payment.createTip', async () => {
      // Validate amount at the service boundary as well, so callers that bypass
      // the route layer (jobs, internal tooling) cannot create bad records.
      const amountCheck = validatePaymentAmount(data.amount);
      if (!amountCheck.valid) {
        throw new ValidationError(amountCheck.reason || 'Amount must be greater than 0');
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

      // #175 — Check on-chain balance before creating any DB record
      const assetCode = requestedAsset?.code ?? 'USDC';
      let walletBalances: { lumens: string; usdc?: string };
      try {
        walletBalances = await getWalletBalance(wallet.publicKey);
      } catch (err) {
        logger.error(
          { userId, walletPublicKey: wallet.publicKey, failureReason: 'balance_check_failed' },
          'Tip blocked: failed to fetch on-chain wallet balance'
        );
        throw new ServiceUnavailableError('Unable to verify wallet balance at this time');
      }

      const rawBalance =
        assetCode === 'USDC' ? (walletBalances.usdc ?? '0') : walletBalances.lumens;
      const currentBalance = parseFloat(rawBalance);
      if (!Number.isFinite(currentBalance) || currentBalance < data.amount) {
        logger.warn(
          {
            userId,
            walletPublicKey: wallet.publicKey,
            requiredAmount: data.amount,
            currentBalance,
            currency: assetCode,
            failureReason: 'insufficient_balance',
          },
          'Tip blocked: insufficient on-chain balance'
        );
        throw new PaymentRequiredError('Insufficient funds to complete the payment', {
          requiredAmount: data.amount,
          currentBalance,
          currency: assetCode,
        });
      }

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
        tip = await this.transactionService.createTip({
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
              ? { media: { connect: attachableMedia.map((media) => ({ id: media.id })) } }
              : {}),
          } as any,
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
        currency: tip.assetCode,
        fromUserId: tip.fromUserId,
        creatorId: tip.creatorId,
      });

      return this.formatTipResponse(tip);
    });
  }

  /**
   * Format a tip record into a TipResponse.
   */
  private formatTipResponse(tip: Tip & { media?: MediaView[] }): TipResponse {
    return {
      id: tip.id,
      fromUserId: tip.fromUserId,
      creatorId: tip.creatorId,
      amount: tip.amount.toString(),
      message: tip.message,
      status: tip.status as TipStatus,
      transactionHash: tip.transactionHash,
      createdAt: tip.createdAt,
      updatedAt: tip.updatedAt,
    } as TipResponse;
  }

  /**
   * Resolve media ids that can be attached to a tip by the given user.
   */
  private async resolveAttachableMedia(userId: string, mediaIds: string[]): Promise<MediaView[]> {
    const media = await this.prisma.media.findMany({
      where: {
        id: { in: mediaIds },
        uploaderId: userId,
        status: 'ready',
        tipId: null,
      },
    });
    return media as unknown as MediaView[];
  }

  /**
   * Update tip status (e.g. after Stellar confirmation).
   */
  async updateTipStatus(tipId: string, request: UpdateTipStatusRequest): Promise<TipResponse> {
    return this.executeWithLogging('payment.updateTipStatus', async () => {
      const tip = await this.transactionService.updateTipStatus(tipId, {
        status: request.status as TipStatus,
        transactionHash: request.transactionHash,
      });
      if (!tip) {
        throw new PaymentNotFoundError();
      }
      await invalidateCaches([tipCacheKeys.tip(tipId)]);
      return this.formatTipResponse(tip);
    });
  }

  /**
   * Get a tip by id.
   */
  async getTip(tipId: string): Promise<TipResponse> {
    return this.executeWithLogging('payment.getTip', async () => {
      const tip = await this.prisma.tip.findFirst({
        where: { id: tipId, ...VISIBLE_TIPS_ONLY },
        select: TIP_RESPONSE_SELECT,
      });
      if (!tip) {
        throw new PaymentNotFoundError();
      }
      return this.formatTipResponse(tip as unknown as Tip & { media?: MediaView[] });
    });
  }

  /**
   * List tips with filters and pagination.
   */
  async listTips(filters: TipFilterInput): Promise<{ data: TipResponse[]; total: number }> {
    return this.executeWithLogging('payment.listTips', async () => {
      const where = buildTipWhere(filters);
      const pageSize = sanitizePageSize(filters.limit);
      const page = sanitizePageNumber(filters.page);
      const orderBy = parseSortParameters(filters.sort);
      const [total, tips] = await this.prisma.$transaction([
        this.prisma.tip.count({ where }),
        this.prisma.tip.findMany({
          where,
          select: TIP_RESPONSE_SELECT,
          orderBy,
          skip: (page - 1) * pageSize,
          take: pageSize,
        }),
      ]);
      logger.debug(`Listed tips with filters: ${describeTipFilters(filters)}`);
      return {
        data: tips.map((tip) => this.formatTipResponse(tip as unknown as Tip & { media?: MediaView[] })),
        total,
      };
    });
  }

  /**
   * Build an unsigned Stellar payment transaction for a tip.
   */
  async buildPaymentTransaction(tipId: string, userId: string): Promise<BuildTransactionResponse> {
    return this.executeWithLogging('payment.buildPaymentTransaction', async () => {
      const tip = await this.prisma.tip.findFirst({
        where: { id: tipId, ...VISIBLE_TIPS_ONLY },
      });
      if (!tip) {
        throw new PaymentNotFoundError();
      }
      if (tip.fromUserId !== userId) {
        throw new UnauthorizedError('Tip does not belong to user');
      }
      if (tip.status !== TipStatus.PENDING) {
        throw new ConflictError(`Tip is not pending (current status: ${tip.status})`);
      }

      const wallet = await this.prisma.wallet.findFirst({
        where: { userId, verified: true },
        select: { publicKey: true },
      });
      if (!wallet) {
        throw new ValidationError('User does not have a verified wallet');
      }

      const creator = await this.prisma.creator.findUnique({
        where: { id: tip.creatorId },
        select: { user: { select: { walletAddress: true } } },
      });
      const destination = creator.user.walletAddress;
      if (!destination) {
        throw new ValidationError('Creator has no wallet address');
      }

      const memo = buildTipMemo(tip.id);
      const memoCheck = validateMemo(memo);
      if (!memoCheck.valid) {
        throw new ValidationError(memoCheck.reason || 'Invalid memo');
      }

      const built = await buildPaymentTransaction({
        sourcePublicKey: wallet.publicKey,
        destinationPublicKey: destination,
        amount: tip.amount.toString(),
        assetCode: tip.assetCode ?? 'USDC',
        assetIssuer: tip.assetIssuer ?? undefined,
        memo: memoCheck.memo ?? memo,
        networkPassphrase: config.STELLAR_NETWORK_PASSTHRASE,
      });

      return {
        tipId,
        transactionXdr: built.xml,
        networkPassphrase: config.STELLAR_NETWORK_PASSTHRASE, 
      } as BuildTransactionResponse;
    });
  }

  /**
   * Submit a signed payment transaction and update the tip status.
   */
  async submitPaymentTransaction(tipId: string, userId: string, signedXdr: string): Promise<SubmitTransactionResponse> {
    return this.executeWithLogging('payment.submitPaymentTransaction', async () => {
      const tip = await this.prisma.tip.findFirst({
        where: { id: tipId, ...VISIBLE_TIPS_ONLY },
      });
      if (!tip) {
        throw new PaymentNotFoundError();
      }
      if (tip.fromUserId !== userId) {
        throw new UnauthorizedError('Tip does not belong to user');
      }
      if (tip.status !== TipStatus.PENDING) {
        throw new ConflictError(`Tip is not pending (current status: ${tip.status})`);
      }

      const result = await submitSignedTransaction(signedXdr, {
        networkPassphrase: config.STELLAR_NETWORK_PASSPHRASE,
      });

      await this.transactionService.updateTipStatus(tipId, {
        status: TipStatus.CONFIRMED,
        transactionHash: result.hash,
      });
      await invalidateCaches([tipCacheKeys.tip(tipId)]);
      await new WebhookService(this.prisma).dispatchEvent(tip.creatorId, tipId, 'tip.confirmed', {
        tipId,
        transactionHash: result.hash,
      });

      return {
        tipId,
        transactionHash: result.hash,
        status: TipStatus.CONFIRMED,
      } as SubmitTransactionResponse;
    });
  }
}
