import { Prisma, PrismaClient } from '@prisma/client';
import type { PrismaClient as PrismaClientType } from '@prisma/client';

/** Shared client for modules that are not constructed through application bootstrap. */
export const prisma: PrismaClientType = new PrismaClient();

/**
 * Type for the transaction client passed to interactive transaction callbacks.
 * Exported so service layers can accept a transaction scope without creating nested
 * $transaction() calls.
 */
export type TransactionClient = Prisma.TransactionClient;
