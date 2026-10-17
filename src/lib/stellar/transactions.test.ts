import { describe, it, expect, vi } from 'vitest';
import { Keypair } from '@stellar/stellar-sdk';
import { buildPaymentTransaction } from './transactions';
import { MEMO_MAX_LENGTH } from './validation';

const sender = Keypair.random().publicKey();
const recipient = Keypair.random().publicKey();

// Horizon is never contacted: the client hands back a local account so the
// builder can be exercised offline.
vi.mock('./client', async () => {
  const sdk = await import('@stellar/stellar-sdk');
  return {
    getStellarClient: () => ({
      getServer: () => ({
        loadAccount: async (publicKey: string) => new sdk.Account(publicKey, '1'),
      }),
      getNetworkPassphrase: () => sdk.Networks.TESTNET,
      getNetworkType: () => 'testnet',
    }),
  };
});

describe('buildPaymentTransaction memo limit (#180)', () => {
  const payment = (memo: string) => ({
    senderPublicKey: sender,
    recipientPublicKey: recipient,
    amount: '1',
    memo,
  });

  it('accepts a memo of exactly 28 bytes', async () => {
    const memo = 'a'.repeat(MEMO_MAX_LENGTH);

    const transaction = await buildPaymentTransaction(payment(memo));

    expect(transaction.source).toBe(sender);
    expect(transaction.memo.type).toBe('memo_text');
    expect(Buffer.from(transaction.memo.value).toString('utf8')).toBe(memo);
  });

  it('rejects a memo over 28 bytes even when it is under 28 characters', async () => {
    // 15 two byte characters: 15 characters, 30 bytes.
    await expect(buildPaymentTransaction(payment('é'.repeat(15)))).rejects.toThrow(
      `Memo text must be ${MEMO_MAX_LENGTH} bytes or less`
    );
  });
});
