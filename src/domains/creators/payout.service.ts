import { PrismaClient } from '@prisma/client';
import { BaseService } from '../../services/base.service';
import type { CreatorPayoutRequest } from './creator.types';
import { ValidationError, NotFoundError } from '../../utils/errors';
import { logger } from '../../utils/logger';
import { getStellarClient } from '../../lib/stellar/client';
import * as StellarSdk from '@stellar/stellar-sdk';
import { config } from '../../config/env';
import { invalidateCaches, earningsCacheKey, creatorCacheKeys } from '../../lib/cache/invalidation';
import { TransactionService, createTransactionService } from '../../services/transaction.service';

export class PayoutService extends BaseService {
  private readonly txService: TransactionService;

  constructor(private prisma: PrismaClient) {
    super();
    this.txService = createTransactionService(prisma);
  }

  /**
   * Process payout from pending balance to total earnings
   * This creates a payout record and queues it for processing
   */
  async processPayout(creatorId: string, data: CreatorPayoutRequest): Promise<any> {
    return this.executeWithLogging('payout.process', async () => {
      const creator = await this.prisma.creator.findUnique({
        where: { id: creatorId },
        include: {
          user: true,
        },
      });

      if (!creator) {
        throw new NotFoundError('Creator');
      }

      // Validate amount doesn't exceed pending balance
      if (data.amount > creator.pendingBalance) {
        throw new ValidationError(
          `Payout amount exceeds pending balance. Available: ${creator.pendingBalance}`
        );
      }

      // Get creator's verified wallet
      const wallet = await this.prisma.wallet.findFirst({
        where: {
          userId: creator.userId,
          verified: true,
        },
      });

      if (!wallet) {
        throw new ValidationError('No verified wallet found. Please link and verify a wallet first.');
      }

      // Validate minimum payout amount (configurable, default $10 or 50 XLM)
      const minimumPayout = config.MIN_PAYOUT_AMOUNT || 50;
      if (data.amount < minimumPayout) {
        throw new ValidationError(
          `Payout amount must be at least ${minimumPayout} XLM`
        );
      }

      // Create payout record via transaction service (balance check + insert atomic)
      const payout = await this.txService.createPayout({
        creatorId,
        amount: data.amount,
        walletAddress: wallet.publicKey,
      });

      logger.info(`Payout request created for creator ${creatorId}: ${data.amount} to ${wallet.publicKey}`);

      // Queue payout for processing (in production, this would go to a worker queue)
      // For now, process synchronously for simplicity
      await this.executePayoutTransaction(payout.id);

      return {
        id: payout.id,
        amount: payout.amount,
        status: payout.status,
        walletAddress: payout.walletAddress,
      };
    });
  }

  /**
   * Execute the actual Stellar transaction for a payout
   * This is called by the worker queue in production
   */
  private async executePayoutTransaction(payoutId: string): Promise<void> {
    return this.executeWithLogging('payout.executeTransaction', async () => {
      const payout = await this.prisma.payout.findUnique({
        where: { id: payoutId },
        include: {
          creator: true,
        },
      });

      if (!payout) {
        throw new NotFoundError('Payout');
      }

      if (payout.status !== 'pending') {
        logger.warn(`Payout ${payoutId} is not in pending state: ${payout.status}`);
        return;
      }

      // Transition to processing via transaction service (guarded on pending)
      const transitioned = await this.txService.markPayoutProcessing(payoutId);
      if (!transitioned) {
        logger.warn(`Payout ${payoutId} was not pending when attempting to process; skipping`);
        return;
      }

      try {
        const stellarClient = getStellarClient();
        const serverKeypair = stellarClient.getServerKeypair();

        if (!serverKeypair) {
          throw new Error('Server keypair not configured - payout unavailable');
        }

        const server = stellarClient.getServer();
        const networkPassphrase = stellarClient.getNetworkPassphrase();

        // Load server account
        const serverAccount = await server.loadAccount(serverKeypair.publicKey());

        // Build payment transaction
        const transaction = new StellarSdk.TransactionBuilder(serverAccount as any, {
          fee: StellarSdk.BASE_FEE,
          networkPassphrase: networkPassphrase,
          timebounds: {
            minTime: 0,
            maxTime: Math.floor(Date.now() / 1000) + 300, // 5 minute validity
          },
        })
          .addOperation(
            StellarSdk.Operation.payment({
              destination: payout.walletAddress,
              asset: StellarSdk.Asset.native(),
              amount: payout.amount.toString(),
            })
          )
          .build();

        // Sign with server key
        transaction.sign(serverKeypair);

        const transactionEnvelope = transaction.toEnvelope().toXDR() as any;

        // Submit transaction
        const result = await stellarClient.submitTransaction(transactionEnvelope);

        logger.info(`Payout transaction submitted successfully: ${result.id}`);

        // Atomically mark completed + decrement creator pending balance
        await this.txService.completePayout(payout, result.id);

        logger.info(`Payout completed for creator ${payout.creatorId}: ${payout.amount} XLM`);
        await invalidateCaches(
          [earningsCacheKey(payout.creatorId), ...creatorCacheKeys(payout.creatorId)],
          'payout.executed'
        );

        // Dispatch webhook event
        await this.dispatchPayoutWebhook(payout, result.id);

      } catch (error: any) {
        logger.error(`Payout transaction failed for ${payoutId}:`, error);

        // Mark payout as failed via transaction service
        await this.txService.failPayout(
          payoutId,
          error.message || 'Unknown error',
          this.calculateNextRetry(payout.retryCount + 1)
        );

        throw error;
      }
    });
  }

  /**
   * Calculate next retry time with exponential backoff
   */
  private calculateNextRetry(retryCount: number): Date {
    const baseDelay = 5 * 60 * 1000; // 5 minutes
    const maxDelay = 24 * 60 * 60 * 1000; // 24 hours
    const delay = Math.min(baseDelay * Math.pow(2, retryCount), maxDelay);
    return new Date(Date.now() + delay);
  }

  /**
   * Dispatch webhook event for completed payout
   */
  private async dispatchPayoutWebhook(payout: any, transactionHash: string): Promise<void> {
    try {
      const webhooks = await this.prisma.webhook.findMany({
        where: {
          creatorId: payout.creatorId,
          active: true,
        },
      });

      for (const webhook of webhooks) {
        const events = webhook.events as string[];
        if (events.includes('payout.completed')) {
          await this.prisma.webhookEvent.create({
            data: {
              webhookId: webhook.id,
              eventType: 'payout.completed',
              payload: JSON.stringify({
                payoutId: payout.id,
                amount: payout.amount,
                transactionHash,
                walletAddress: payout.walletAddress,
                timestamp: new Date().toISOString(),
              }),
              status: 'pending',
            },
          });
        }
      }

      logger.info(`Webhook events dispatched for payout ${payout.id}`);
    } catch (error) {
      logger.error(`Failed to dispatch webhook for payout ${payout.id}:`, error);
      // Don't fail the payout if webhook dispatch fails
    }
  }

  /**
   * Retry failed payouts
   * This is called by a scheduled worker
   */
  async retryFailedPayouts(): Promise<void> {
    return this.executeWithLogging('payout.retryFailed', async () => {
      const failedPayouts = await this.prisma.payout.findMany({
        where: {
          status: 'failed' as any,
          nextRetryAt: {
            lte: new Date(),
          },
          retryCount: {
            lt: 5, // Max 5 retries
          },
        },
        take: 10, // Process in batches
      });

      logger.info(`Retrying ${failedPayouts.length} failed payouts`);

      for (const payout of failedPayouts) {
        try {
          await this.executePayoutTransaction(payout.id);
        } catch (error) {
          logger.error(`Failed to retry payout ${payout.id}:`, error);
        }
      }
    });
  }

  /**
   * Get payout history for a creator
   */
  async getPayoutHistory(creatorId: string, limit = 20): Promise<any[]> {
    return this.executeWithLogging('payout.getHistory', async () => {
      const payouts = await this.prisma.payout.findMany({
        where: { creatorId },
        orderBy: { createdAt: 'desc' },
        take: limit,
      });

      return payouts.map((payout) => ({
        id: payout.id,
        amount: payout.amount,
        status: payout.status,
        transactionHash: payout.transactionHash,
        walletAddress: payout.walletAddress,
        createdAt: payout.createdAt.toISOString(),
        errorMessage: payout.errorMessage,
      }));
    });
  }

  /**
   * Get creator's earnings breakdown
   */
  async getEarningsBreakdown(creatorId: string): Promise<any> {
    return this.executeWithLogging('payout.getEarningsBreakdown', async () => {
      const creator = await this.prisma.creator.findUnique({
        where: { id: creatorId },
      });

      if (!creator) {
        throw new NotFoundError('Creator');
      }

      // Count total tips
      const tipStats = await this.prisma.tip.aggregate({
        where: {
          creatorId,
          status: 'completed',
        },
        _count: true,
        _sum: {
          amount: true,
        },
      });

      const paidOut = creator.totalEarnings - creator.pendingBalance;

      return {
        totalEarnings: creator.totalEarnings,
        pendingBalance: creator.pendingBalance,
        paidOut,
        totalTips: tipStats._count,
        totalTipAmount: tipStats._sum.amount || 0,
      };
    });
  }

  /**
   * Get creator dashboard with recent tips and earnings
   */
  async getCreatorDashboard(creatorId: string): Promise<any> {
    return this.executeWithLogging('payout.getCreatorDashboard', async () => {
      const creator = await this.prisma.creator.findUnique({
        where: { id: creatorId },
        include: {
          user: {
            select: {
              email: true,
              name: true,
            },
          },
        },
      });

      if (!creator) {
        throw new NotFoundError('Creator');
      }

      // Get recent tips
      const recentTips = await this.prisma.tip.findMany({
        where: {
          creatorId,
        },
        include: {
          fromUser: {
            select: {
              name: true,
            },
          },
        },
        orderBy: {
          createdAt: 'desc',
        },
        take: 10,
      });

      // Count total tips
      const totalTips = await this.prisma.tip.count({
        where: {
          creatorId,
        },
      });

      const paidOut = creator.totalEarnings - creator.pendingBalance;

      return {
        creator: {
          id: creator.id,
          username: creator.username,
          displayName: creator.displayName,
          bio: creator.bio,
          avatar: creator.avatar,
          verified: creator.verified,
          isPublic: creator.isPublic,
        },
        totalTips,
        recentTips: recentTips.map((tip) => ({
          id: tip.id
        })),
      };
    });
  }
}
