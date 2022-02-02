import assert from 'node:assert';
import { createReadStream, promises as fs } from 'node:fs';
import * as csv from 'csv';

const transactionsPath = './input.csv';

function toCointrackerDate(date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, '0');
  const day = `${date.getDate()}`.padStart(2, '0');
  const hours = `${date.getHours()}`.padStart(2, '0');
  const minutes = `${date.getMinutes()}`.padStart(2, '0');
  const seconds = `${date.getSeconds()}`.padStart(2, '0');
  return `${month}/${day}/${year} ${hours}:${minutes}:${seconds}`;
}

async function main() {
  const records = [];
  const readStream = createReadStream(transactionsPath).pipe(csv.parse());

  for await (const record of readStream) {
    records.push(record);
  }

  console.log(`Found ${records.length} records`);

  // Remove headings.
  records.shift();

  const txns = new Map();
  for (let i = 0; i < records.length; i++) {
    const [userId, utcTime, account, operation, coin, change, remark] = records[i];
    const date = new Date(utcTime);
    const seconds = Math.round(date.getTime() / 1000);

    // Transactions don't always have the same exact second timestamp, so look for +/- 1 second.
    let txn;
    if (txns.has(seconds)) {
      txn = txns.get(seconds);
    } else if (txns.has(seconds - 1)) {
      txn = txns.get(seconds - 1);
    } else if (txns.has(seconds + 1)) {
      txn = txns.get(seconds + 1);
    } else {
      txn = {
        Date: toCointrackerDate(date),
        Remark: remark,
      };
    }

    const debugString = `Received ${change} instead for ${coin} transaction on ${utcTime}.`;

    // Trading "sent" values.
    if (operation === 'Transaction Related') {
      // Change should be negative because it's the money spent.
      assert(change < 0, `Trade change should be negative. ${debugString}`);
      txn = {
        ...txn,
        'Sent Quantity': Math.abs(change),
        'Sent Currency': coin,
      };
    } else if (operation === 'Fee') {
      // Fees
      // Fee should be negative because it's the money spent.
      assert(change < 0, `Fee change should be negative. ${debugString}`);
      txn = {
        ...txn,
        'Fee Amount': Math.abs(change),
        'Fee Currency': coin,
      };
    } else if (operation === 'Buy') {
      // Received value.
      assert(change > 0, `Buy change should be positive. ${debugString}`);
      txn = {
        ...txn,
        'Received Quantity': Math.abs(change),
        'Received Currency': coin,
      };
    } else if (operation === 'Distribution') {
      // Airdrop.
      // assert(change > 0, `Airdrop should be positive. ${debugString}`);
      if (change < 0) {
        // I had a negative airdrop when Binance sold the delisted BCPT token.
        // Koinly shows this as a "Send", so that's what we're doing here.
        txn = {
          ...txn,
          'Sent Quantity': Math.abs(change),
          'Sent Currency': coin,
        };
      } else {
        txn = {
          ...txn,
          'Received Quantity': Math.abs(change),
          'Received Currency': coin,
          Tag: 'airdrop',
        };
      }
    } else if (operation === 'Commission History' || operation === 'Commission Rebate') {
      // Staking?
      assert(change > 0, `Stake should be positive. ${debugString}`);
      txn = {
        ...txn,
        'Received Quantity': Math.abs(change),
        'Received Currency': coin,
        Tag: 'staked',
      };
    } else if (operation === 'Deposit') {
      // Deposit.
      assert(change > 0, `Deposit should be positive. ${debugString}`);
      txn = {
        ...txn,
        'Received Quantity': Math.abs(change),
        'Received Currency': coin,
      };
    } else if (operation === 'Withdraw') {
      // Withdrawal.
      assert(change < 0, `Withdrawal should be negative. ${debugString}`);
      txn = {
        ...txn,
        'Sent Quantity': Math.abs(change),
        'Sent Currency': coin,
      };
    } else {
      console.log(`ignored operation ${operation}`);
    }

    txns.set(seconds, txn);
  }

  console.log(`Transformed to ${txns.size} records`);

  const newRecords = Array.from(txns.values());

  const result = csv.stringify(newRecords, {
    header: true,
    columns: [
      'Date',
      'Received Quantity',
      'Received Currency',
      'Sent Quantity',
      'Sent Currency',
      'Fee Amount',
      'Fee Currency',
      'Tag',
      // 'Remark', // Not part of Cointracker CSV format.
    ],
  });

  await fs.writeFile('./output.csv', result);
}

main();
