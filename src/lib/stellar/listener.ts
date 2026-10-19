import { PrismaClient } from '@prisma/client';
import { getStellarClient } from './client';
import { logger } from '../../utils/logger';
import {
  transactionService,
  TipConfirmationResult,
} from '../../services/transaction.service';

export interface TransactionListener {
  tipId: string;
  transactionHash: string;
  onConfirmed: (result: any) => void;
  onFailed: (error: any) => void;
}

/**
 * Poll for transaction confirmation on Horizon
 * Checks periodically if a transaction has been confirmed on the Stellar network
 *
 * @param transactionHash Transaction to monitor
 * @param maxAttempts Maximum polling attempts (default 60 = ~~60 seconds at 1s interval)
 * @param pollIntervalMs Interval between polls in milliseconds (default 1000)
 * @returns Confirmation status with transaction details if confirmed
 */
export async function pollForTransactionConfirmation(
  transactionHash: string,
  maxAttempts: number = 60,
  pollIntervalMs: number = 1000
): Promise<{
  confirmed: boolean;
  result?: any;
  error?: string;
}> {
  try {
    logger.debug(
      `Starting transaction confirmation polling: ${transactionHash} (max ${maxAttempts} attempts)`
    );

    const client = getStellarClient();
    const server = client.getServer();

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        const transaction = await server.transactions().hash(transactionHash).call();

        if (transaction) {
          logger.info(
            `Transaction confirmed: ${transactionHash} at ledger ${transaction.ledger} (attempt ${attempt + 1}/${maxAttempts})`
          );
          return {
            confirmed: true,
            result: transaction,
          };
        }
      } catch (error: any) {
        if (error.status !== 404) {
          // 404 is expected while transaction is pending
          logger.error(`Unexpected error polling transaction ${transactionHash}:`, error);
          throw error;
        }

        // 404 means transaction not yet confirmed, continue polling
        logger.debug(`Transaction not yet confirmed (attempt ${attempt + 1}/${maxAttempts})`);
      }

      // Wait before next poll (except on last attempt)
      if (attempt < maxAttempts - 1) {
        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
      }
    }

    logger.warn(
      `Transaction confirmation timeout after ${maxAttempts} attempts: ${transactionHash}`
    );
    return {
      confirmed: false,
      error: `Transaction not confirmed after ${maxAttempts} polling attempts`,
    };
  } catch (error) {
    logger.error(`Failed to poll for transaction confirmation: ${transactionHash}`, error);
    throw error;
  }
}

/**
 * Update tip status based on transaction confirmation
 * Used to transition tip from pending to completed/failed based on network confirmation
 *
 * This function delegates all state mutations to the centralized transaction
 * service so that confirmation handling is atomic and nesting-free.
 *
 * @param prisma Prisma client instance
 * @param tipId Tip to update
 * @param transactionHash Transaction hash to verify
 * @returns Updated tip or throws on error
 */
export async function updateTipStatusFromTransaction(
  prisma: PrismaClient,
  tipId: string,
  transactionHash: string
): Promise<void> {
  try {
    logger.debug(
      `Updating tip status based on transaction confirmation: ${tipId} (${transactionHash})`
    );

    // Poll for transaction confirmation
    const confirmationResult = await pollForTransactionConfirmation(transactionHash);

    if (confirmationResult.confirmed && confirmationResult.result) {
      // Transaction is confirmed on the network - complete the tip atomically
      const credited = await transactionService.completeTipFromConfirmation(
        prisma,
        tipId,
        confirmationResult.result
      );

      if (credited) {
        logger.info(`Tip confirmed and completed: ${tipId}`);
      }
    } else {
      // Transaction failed or timed out - mark as failed atomically
      const marked = await transactionService.markTipFailed(prisma, tipId, {
        reason: confirmationResult.error,
      });

      if (marked) {
        logger.warn(`Tip marked as failed: ${tipId}, reason: ${confirmationResult.error}`);
      }
    }
  } catch (error) {
    logger.error(`Failed to update tip status from transaction: ${tipId}`, error);

    // Attempt to mark as failed to prevent hanging pending status
    try {
      const marked = await transactionService.markTipFailed(prisma, tipId, {
        reason: error instanceof Error ? error.message : String(error),
      });

      if (marked) {
        logger.warn(`Tip marked as failed due to error: ${tipId}`);
      }
    } catch (updateError) {
      logger.error(`Failed to mark tip as failed: ${tipId}`, updateError);
    }
  }
}

/**
 * Stream and process transactions for a creator account
 * Used to detect incoming USDC payments to a creator's wallet
 * Updates corresponding tips in real-time when payments are detected
 *
 * @param prisma Prisma client instance
 * @param creatorPublicKey Creator's Stellar public key
 * @param targetAssetCode Optional: only process payments for this asset (e.g., 'USDC')
 * @returns Function to close the stream
 */
export async function streamCreatorPayments(
  prisma: PrismaClient,
  creatorPublicKey: string,
  targetAssetCode?: string
): Promise<() => void> {
  try {
    logger.debug(
      `Starting payment stream for creator: ${creatorPublicKey}${targetAssetCode ? ` (asset: ${targetAssetCode})` : ''}`
    );

    const client = getStellarClient();
    const server = client.getServer();

    const closeStream = await server
      .payments()
      .forAccount(creatorPublicKey)
      .stream({
        onmessage: async (payment: any) => {
          try {
            // Filter for payments (not other operations)
            if (payment.type !== 'payment') {
              return;
            }

            // Filter for target asset if specified
            if (targetAssetCode && payment.asset_code !== targetAssetCode) {
              return;
            }

            logger.debug(
              `Received payment for creator: ${creatorPublicKey}, tx: ${payment.transaction_hash}, amount: ${payment.amount}`
            );

            // Find matching tip by transaction hash
            const tip = await prisma.tip.findFirst({
              where: {
                transactionHash: payment.transaction_hash,
                status: 'pending',
              },
            });

            if (tip) {
              logger.info(`Confirming tip from payment stream: ${tip.id}`);

              // Atomic transition + earnings credit via the transaction service
              const credited = await transactionService.completeTipFromConfirmation(
                prisma,
                tip.id,
                payment
              );

              if (credited) {
                logger.info(
                  `Tip auto-confirmed from payment stream: ${tip.id}, amount: ${tip.amount}`
                );
              }
            } else {
              logger.debug(
                `No matching pending tip found for payment: ${payment.transaction_hash}`
              );
            }
          } catch (error) {
            logger.error('Error processing payment from stream:', error);
          }
        },

        onerror: (error: any) => {
          logger.error(`Payment stream error for creator ${creatorPublicKey}:`, error);
        },
      });

    logger.info(`Payment stream started for creator: ${creatorPublicKey}`);
    return closeStream;
  } catch (error) {
    logger.error(`Failed to start payment stream for creator: ${creatorPublicKey}`, error);
    throw error;
  }
}

/**
 * Background job to process pending tips
 * Polls Horizon for confirmation of pending transactions
 * Call this periodically (e.g., every 30 seconds) to confirm pending tips
 *
 * @param prisma Prisma client instance
 * @returns Number of tips processed
 */
export async function processPendingTips(prisma: PrismaClient): Promise<number> {
  try {
    logger.debug('Processing pending tips');

    // Find all pending tips with transaction hashes
    const pendingTips = await prisma.tip.findMany({
      where: {
        status: 'pending',
        transactionHash: {
          not: null,
        },
      },
      take: 50, // Process up to 50 at a time to avoid overload
    });

    logger.debug(`Found ${pendingTips.length} pending tips to process`);

    let confirmed = 0;
    let failed = 0;

    // Process each tip
    for (const tip of pendingTips) {
      if (tip.transactionHash) {
        try {
          const confirmationResult = await pollForTransactionConfirmation(
            tip.transactionHash,
            5, // Fewer attempts for background job (5 seconds)
            500 // Shorter interval (500ms)
          );

          if (confirmationResult.confirmed && confirmationResult.result) {
            // Atomic transition + earnings credit via the transaction service
            const credited = await transactionService.completeTipFromConfirmation(
              prisma,
              tip.id,
              confirmationResult.result
            );

            if (credited) {
              confirmed++;
              logger.info(`Confirmed pending tip: ${tip.id}`);
            }
          } else {
            // Check if too much time has passed (give up after 5 minutes)
            const ageMs = Date.now() - tip.createdAt.getTime();
            if (ageMs > 5 * 60 * 1000) {
              // Mark as failed if over 5 minutes old
              const marked = await transactionService.markTipFailed(prisma, tip.id, {
                reason: 'Transaction not confirmed within 5 minutes',
              });

              if (marked) {
                failed++;
                logger.warn(`Gave up on pending tip after 5 minutes: ${tip.id}`);
              }
            }
          }
        } catch (error) {
          logger.error(`Error processing pending tip ${tip.id}:`, error);
        }
      }
    }

    logger.info(
      `Finished processing pending tips: ${confirmed} confirmed, ${failed} failed, ${pendingTips.length - confirmed - failed} still pending`
    );

    return confirmed + failed;
  } catch (error) {
    logger.error('Failed to process pending tips:', error);
    return 0;
  }
}

/**
 * Initialize background job for processing pending tips
 * Sets up an interval to periodically check for confirmation of pending tips
 *
 * @param prisma Prisma client instance
 * @param intervalMs Interval between job runs in milliseconds (default 30000 = 30 seconds)
 * @returns Function to stop the background job
 */
export function initializePendingTipsJob(
  prisma: PrismaClient,
  intervalMs: number = 30000
): () => void {
  logger.info(`Initializing pending tips job (interval: ${intervalMs}ms)`);

  const interval = setInterval(() => {
    processPendingTips(prisma).catch((error) => {
      logger.error('Unhandled error in pending tips job:', error);
    });
  }, intervalMs);

  // Run once immediately on startup
  processPendingTips(prisma).catch((error) => {
    logger.error('Unhandled error in initial pending tips job:', error);
  });

  return () => {
    clearInterval(interval);
    logger.info('Pending tips job stopped');
  };
}

/**
 * Convenience helper to complete a tip via the transaction service.
 * Delegates to `transactionService.completeTipWithResult` and returns
 * the standardized confirmation result for callers that need it.
 */
export async function confirmTipFromTransaction(
  prisma: PrismaClient,
  tipId: string,
  confirmation: unknown
): Promise<TipConfirmationResult> {
  return transactionService.completeTipWithResult(prisma, tipId, confirmation);
}
