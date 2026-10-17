import { PrismaClient } from '@prisma/client';

/** Shared client for modules that are not constructed through application bootstrap. */
export const prisma = new PrismaClient();

/**
 * Type for the transaction client passed to interactive transaction callbacks.
 * Exported so service layers can accept a transaction handle without nesting `$transaction()` calls.
 */
export type PrismaTransactionClient = Omit<PrismaClient, '$connect' | '$disconnect' | '$transaction' | '$extend'>;

/**
 * Alias used by the transaction service to accept either the root client or an
 * active transaction handle, enabling atomic composition without nested `$transaction()` calls.
 */
export type PrismaClientOrTransaction = PrismaClient | PrismaTransactionClient;

/**
 * Array-form transaction input type, used by the transaction service to batch
 * dependent operations atomically without nested `$transaction()` calls.
 */
export type TransactionArray = Parameters<PrismaClient['$transaction']>[0];

/**
 * Callback signature for interactive transactions executed by the transaction service.
 */
export type TransactionCallback<T> = (tx: PrismaTransactionClient) => Promise<T>;
