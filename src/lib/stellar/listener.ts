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
 * Checks periodically if a transaction has been confirmed on the Stellar Network
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
 * Resolve the transaction service to use. Allows tests to inject a custom
 * instance while production code falls back to the shared singleton.
 */
function resolveTransactionService(
  prisma: PrismaClient,
  service?: TransactionService
): TransactionService {
  if (service) {
    return service;
  }
  // Reuse the shared singleton when it wraps the same Prisma client.
  if (transactionService.getClient() === prisma) {
    return transactionService;
  }
  return new TransactionService(prisma);
}

/**
 * Update tip status based on transaction confirmation
 * Used to transition tip from pending to completed/failed based on network confirmation
 *
 * The actual state mutation is delegated to the transaction service, which
 * guarantees atomicity and idempotency across competing confirmation paths.
 *
 * @param prisma Prisma client instance
 * @param tipId Tip to update
 * @param transactionHash Transaction hash to verify
 * @param service Optional transaction service override (for testing)
 * @returns Updated tip or throws on error
 */
export async function updateTipStatusFromTransaction(
  prisma: PrismaClient,
  tipId: string,
  transactionHash: string,
  service?: TransactionService
): Promise<void> {
  const txService = resolveTransactionService(prisma, service);

  try {
    logger.debug(
      `Updating tip status based on transaction confirmation: ${tipId} (${transactionHash})`
    );

    // Poll for transaction confirmation
    const confirmationResult = await pollForTransactionConfirmation(transactionHash);

    if (confirmationResult.confirmed && confirmationResult.result) {
      // Transaction is confirmed on the network - complete the tip atomically
      const result = await txService.completeTipFromConfirmation(
        tipId,
        transactionHash
      );

      if (result.notFound) {
        logger.warn(`Tip not found when updating from transaction: ${tipId}`);
        return;
      }

      if (result.alreadyResolved) {
        logger.debug(`Tip already has status ${result.status}, skipping update`);
        return;
      }

      if (result.credited) {
        logger.info(
          `Tip confirmed and completed: ${tipId}, creator earnings updated by ${result.amount}`
        );
      }
    } else {
      // Transaction failed or timed out
      const result = await txService.markTipFailed(tipId);

      if (result.notFound) {
        logger.warn(`Tip not found when marking as failed: ${tipId}`);
        return;
      }

      if (result.updated) {
        logger.warn(`Tip marked as failed: ${tipId}, reason: ${confirmationResult.error}`);
      }
    }
  } catch (error) {
    logger.error(`Failed to update tip status from transaction: ${tipId}`, error);

    // Attempt to mark as failed to prevent hanging pending status
    try {
      const result = await txService.markTipFailed(tipId);
      if (result.updated) {
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
 * @param service Optional transaction service override (for testing)
 * @returns Function to close the stream
 */
export async function streamCreatorPayments(
  prisma: PrismaClient,
  creatorPublicKey: string,
  targetAssetCode?: string,
  service?: TransactionService
): Promise<{
  close: () => void;
  closeStream: () => void;
}> {
  try {
    logger.debug(
      `Starting payment stream for creator: ${creatorPublicKey}${targetAssetCode ? ` (asset: ${targetAssetCode})` : ''}`
    );

    const txService = resolveTransactionService(prisma, service);
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

              // Atomic transition + earnings credit via the transaction service.
              const result = await txService.completeTipFromConfirmation(
                tip.id,
                payment.transaction_hash
              );
              if (result.credited) {
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
    return { close: closeStream, closeStream };
  } catch (error) {
    logger.error(`Failed to start payment stream for creator: ${creatorPublicKey}`, error);
    throw error;
  }
}

/**
 * Background job to process pending tips
 * Polls Horizon for confirmation and updates tip statuses accordingly
 */
export async function processPendingTipsConfirmations(
  prisma: PrismaClient,
  service?: TransactionService
): Promise<void> {
  try {
    const pendingTips = await prisma.tip.findMany({
      where: {
        status: 'pending',
        transactionHash: { not: null },
      },
      take: 50,
    });

    logger.debug(`Checking confirmation for ${pendingTips.length} pending tips`);

    for (const tip of pendingTips) {
      if (!tip.transactionHash) {
        continue;
      }
      try {
        await updateTipStatusFromTransaction(prisma, tip.id, tip.transactionHash, service);
      } catch (error) {
        logger.error(`Failed to process confirmation for tip ${tip.id}:`, error);
      }
    }
  } catch (error) {
    logger.error(`Failed to process pending tips confirmations:`, error);
    throw error;
  }
}
