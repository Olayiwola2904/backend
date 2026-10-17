import { PrismaClient } from '@prisma/client';

/** Shared client for modules that are not constructed through application bootstrap. */
export const prisma = new PrismaClient();

/** Transaction client type aliased for use by the transaction service. */
export type TransactionClient = Omit<
  PrismaClient,
  '$connect' | '$disconnect' | '$on' | '$transaction' | '$use' | '$extends'
>;
