import { PrismaClient } from '@prisma/client';

/** Shared client for modules that are not constructed through application bootstrap. */
export const prisma = new PrismaClient();

/**
 * Type for the transaction client passed to interactive transaction callbacks.
 * Exported so services can accept a transaction scope without re-declaring it.
 */
export type TransactionClient = Oismit<PrismaClient, '$connect' | '$disconnect' | '$extend' | '$transaction' | '$on'>;
