import { PrismaClient } from '@prisma/client';
import { getStellarClient } from './client';
import { logger } from '../../utils/logger';
import {
  transactionService,
  TransactionService,
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
 * @param maxAttempts Maximum polling attempts (default 60 = ~60 seconds at 1s interval)
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
 * Resolve the transaction service to use.
 *
 * Callers may inject a service (e.g. for testing or to share a configured
 * Prisma client). When not provided, the shared singleton is used.
 */
function resolveTransactionService(
  service?: TransactionService | PrismaClient
): TransactionService {
  if (!service) {
    return transactionService;
  }
  if (service instanceof TransactionService) {
    return service;
  }
  // Backwards-compatibility: accept a raw PrismaClient and wrap it.
  return new TransactionService(service);
}

/**
 * Update tip status based on transaction confirmation
 * Used to transition tip from pending to completed/failed based on network confirmation
 *
 * All payment state mutations are delegated to the transaction service so that
 * related writes (tip status + creator earnings) execute atomically and nesting is
 * eliminated.
 *
 * @param prisma Prisma client instance (or a TransactionService)
 * @param tipId Tip to update
 * @param transactionHash Transaction hash to verify
 * @returns Updated tip or throws on error
 */
export async function updateTipStatusFromTransaction(
  prisma: PrismaClient | TransactionService,
  tipId: string,
  transactionHash: string
): Promise<void> {
  const service = resolveTransactionService(prisma);

  try {
    logger.debug(
      `Updating tip status based on transaction confirmation: ${tipId} (${transactionHash})`
    );

    // Poll for transaction confirmation
    const confirmationResult = await pollForTransactionConfirmation(transactionHash);

    if (confirmationResult.confirmed && confirmationResult.result) {
      // Transaction is confirmed on the network - complete the tip atomically.
      // The service guarantees the pending -> completed transition and the
      // creator earnings credit execute in a single transaction, and that
      // concurrent confirmers credit earnings exactly once.
      const credited = await service.completeTipAtomically(tipId);
      if (credited) {
        logger.info(`Tip confirmed and completed: ${tipId}`);
      }
    } else {
      // Transaction failed or timed out
      const marked = await service.markTipFailed(tipId);
      if (marked) {
        logger.warn(`Tip marked as failed: ${tipId}, reason: ${confirmationResult.error}`);
      }
    }
  } catch (error) {
    logger.error(`Failed to update tip status from transaction: ${tipId}`, error);

    // Attempt to mark as failed to prevent hanging pending status
    try {
      const marked = await service.markTipFailed(tipId);
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
 * @param prisma Prisma client instance (or a TransactionService)
 * @param creatorPublicKey Creator's Stellar public key
 * @param targetAssetCode Optional: only process payments for this asset (e.g., 'USDC')
 * @returns Function to close the stream
 */
export async function streamCreatorPayments(
  prisma: PrismaClient | TransactionService,
  creatorPublicKey: string,
  targetAssetCode?: string
): Promise<() => void> {
  const service = resolveTransactionService(prisma);

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
            const tip = await service.findPendingTipByTransactionHash(
              payment.transaction_hash
            );

            if (tip) {
              logger.info(`Confirming tip from payment stream: ${tip.id}`);

              // Atomic transition + earnings credit via the transaction service.
              const credited = await service.completeTipAtomically(tip.id);
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
 * @param prisma Prisma client instance (or a TransactionService)
 * @returns Number of tips processed
 */
export async function processPendingTips(
  prisma: PrismaClient | TransactionService
): Promise<number> {
  const service = resolveTransactionService(prisma);

  try {
    logger.debug('Processing pending tips');

    // Find all pending tips with transaction hashes
    const pendingTips = await service.listPendingTipsWithTransaction(50);

    logger.debug(`Found ${pendingTips.length} pending tips to process`);

    let processed = 0;

    for (const tip of pendingTips) {
      if (!tip.transactionHash) {
        continue;
      }

      try {
        const confirmationResult = await pollForTransactionConfirmation(
          tip.transactionHash,
          3,
          500
        );

        if (confirmationResult.confirmed) {
          const credited = await service.completeTipAtomically(tip.id);
          if (credited) {
            processed++;
            logger.info(`Pending tip confirmed: ${tip.id}`);
          }
        }
      } catch (error) {
        logger.error(`Failed to process pending tip ${tip.id}:`, error);
      }
    }

    logger.debug(`Processed ${processed} pending tips`);
    return processed;
  } catch (error) {
    logger.error('Failed to process pending tips:', error);
    throw error;
  }
}
