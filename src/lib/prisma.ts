import { Prisma, PrismaClient } from '@prisma/client';

/** Shared client for modules that are not constructed through application bootstrap. */
export const prisma = new PrismaClient();

/** Transaction client type exposed for the transaction service. */
export type PrismaTransactionClient = Prisma.TransactionClient;

/** Isolation levels supported by the transaction service. */
export type TransactionIsolationLevel =
  | 'ReadUncommitted'
  | 'ReadCommitted'
  | 'RepeatableRead'
  | 'Serializable';
