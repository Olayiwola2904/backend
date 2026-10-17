import { PrismaClient, Prisma } from '@prisma/client';

/** Shared client for modules that are not constructed through application bootstrap. */
export const prisma = new PrismaClient();

/**
 * Transaction isolation levels used by the consolidated transaction service.
 * Re-exported from Prisma so callers and tests share the same definitions as the client.
 */
export { Prisma } from '@prisma/client';
export type TransactionIsolationLevelValue = Prisma.TransactionIsolationLevel;
