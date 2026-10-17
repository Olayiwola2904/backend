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
  service?: TransactionService
): TransactionService {
  return service ?? transactionService;
}

/**
 * Update tip status based on transaction confirmation
 * Used to transition tip from pending to completed/failed based on network confirmation
 *
 * The actual state mutation is delegated to the transaction service, which
 * guarantees atomicity and idempotency (see completeTipAtomically).
 *
 * @param paymentPrisma Prisma client instance (used by the transaction service)
 * @param tipId Tip to update
 * @param transactionHash Transaction hash to verify
 * @param txService Optional transaction service override (for testing)
 * @returns Updated tip or throws on error
 */
export async function updateTipStatusFromTransaction(
  paymentPrisma: PrismaClient,
  tipId: string,
  transactionHash: string,
  txService?: TransactionService
): Promise<void> {
  const service = resolveTransactionService(txService);

  try {
    logger.debug(
      `Updating tip status based on transaction confirmation: ${tipId} (${transactionHash})`
    );

    // Poll for transaction confirmation
    const confirmationResult = await pollForTransactionConfirmation(transactionHash);

    if (confirmationResult.confirmed && confirmationResult.result) {
      // Transaction is confirmed on the network - complete the tip atomically
      const result = await service.completeTip(tipId);

      if (result.status === 'not_found') {
        logger.warn(`Tip not found when updating from transaction: ${tipId}`);
        return;
      }

      if (result.status === 'completed') {
        logger.info(
          `Tip confirmed and completed: ${tipId}, creator earnings updated by ${result.amount}`
        );
      } else if (result.status === 'already_processed') {
        logger.debug(`Tip already has status ${result.currentStatus}, skipping update`);
      }
    } else {
      // Transaction failed or timed out
      const result = await service.failTip(
        tipId,
        confirmationResult.error ?? 'Transaction not confirmed'
      );

      if (result.status === 'not_found') {
        logger.warn(`Tip not found when marking as failed: ${tipId}`);
        return;
      }

      if (result.status === 'failed') {
        logger.warn(`Tip marked as failed: ${tipId}, reason: ${confirmationResult.error}`);
      }
    }
  } catch (error) {
    logger.error(`Failed to update tip status from transaction: ${tipId}`, error);

    // Attempt to mark as failed to prevent hanging pending status
    try {
      const result = await service.failTip(
        tipId,
        error instanceof Error ? error.message : 'Unknown error'
      );

      if (result.status === 'failed') {
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
 * @param paymentPrisma Prisma client instance (used by the transaction service)
 * @param creatorPublicKey Creator's Stellar public key
 * @param targetAssetCode Optional: only process payments for this asset (e.g., 'USDC')
 * @param txService Optional transaction service override (for testing)
 * @returns Function to close the stream
 */
export async function streamCreatorPayments(
  paymentPrisma: PrismaClient,
  creatorPublicKey: string,
  targetAssetCode?: string,
  txService?: TransactionService
): Promise<() => void> {
  const service = resolveTransactionService(txService);

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
            const tip = await paymentPrisma.tip.findFirst({
              where: {
                transactionHash: payment.transaction_hash,
                status: 'pending',
              },
            });

            if (tip) {
              logger.info(`Confirming tip from payment stream: ${tip.id}`);

              // Atomic transition + earnings credit via the transaction service.
              const result = await service.completeTip(tip.id);

              if (result.status === 'completed') {
                logger.info(
                  `Tip auto-confirmed from payment stream: ${tip.id}, amount: ${result.amount}`
                );
              } else if (result.status === 'already_processed') {
                logger.warn(
                  `Concurrent tip confirmation detected for ${tip.id}; earnings credited once`
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
