import { PrismaClient } from '@prisma/client';

/** Shared client for modules that are not constructed through application bootstrap. */
export const prisma = new PrismaClient();

/**
 * Type for the transaction client passed to interactive transaction callbacks.
 * Exported so service layers can accept a transaction scope without creating nested
 * $transaction calls.
 */
export type PrismaTransactionClient = Omit<PrismaClient, '$connect' | '$disconnect' | '$transaction' | '$on' | '$use' | '$extend'>;
