import * as sqlite3 from 'sqlite3';
import * as sqlite from 'sqlite';
import * as pg from 'pg';

import { getPgClientConfig } from '../datastore/postgres-store';

import {
  DbMempoolTx,
  DataStore,
  createDbTxFromCoreMsg,
  DbEventBase,
  DbSmartContractEvent,
  DbStxEvent,
  DbEventTypeId,
  DbFtEvent,
  DbAssetEventTypeId,
  DbNftEvent,
  DbBlock,
  DataStoreBlockUpdateData,
  createDbMempoolTxFromCoreMsg,
  DbStxLockEvent,
  DbMinerReward,
  DbBurnchainReward,
  getTxDbStatus,
  DbRewardSlotHolder,
  DbBnsName,
  DbBnsNamespace,
  DbBnsSubdomain,
  DbMicroblockPartial,
  DataStoreMicroblockUpdateData,
  DataStoreTxEventData,
  DbMicroblock,
} from '../datastore/common';
import {
  CoreNodeMinedBlock,
  CoreNodeBlockMessage,
  CoreNodeEventType,
  CoreNodeBurnBlockMessage,
  CoreNodeDropMempoolTxMessage,
  CoreNodeAttachmentMessage,
  CoreNodeMicroblockMessage,
  CoreNodeParsedTxMessage,
  CoreNodeEvent,
  CoreNodeMinedTransactionEvent,
} from './core-node-message';

import {
  parseArgBoolean,
  parsePort,
  APP_DIR,
  isTestEnv,
  isDevEnv,
  bufferToHexPrefixString,
  hexToBuffer,
  stopwatch,
  timeout,
  logger,
  logError,
  FoundOrNot,
  getOrAdd,
  assertNotNullish,
  batchIterate,
  distinctBy,
  unwrapOptional,
  pipelineAsync,
  isProdEnv,
  has0xPrefix,
  isValidPrincipal,
  isSmartContractTx,
} from '../helpers';


const CREATE_TABLES = [
  `CREATE TABLE IF NOT EXISTS stacks_blocks (
    index_block_hash TEXT PRIMARY KEY,
    anchor_block_hash TEXT,
    parent_index_block_hash TEXT,
    stacks_height INTEGER,
    burn_height INTEGER
   )`,
 "CREATE INDEX IF NOT EXISTS stacks_blocks_burn_height ON stacks_blocks(burn_height)",
 "CREATE INDEX IF NOT EXISTS stacks_blocks_stacks_height ON stacks_blocks(stacks_height)",
 `CREATE TABLE IF NOT EXISTS stacks_txs_confirmed (
   txid TEXT PRIMARY KEY,
   confirmed_height INTEGER,
   fee INTEGER,
   cost_runtime INTEGER,
   cost_write_count INTEGER,
   cost_read_count INTEGER,
   cost_read_len INTEGER,
   cost_write_len INTEGER,
   tx_len INTEGER,
   origin_nonce INTEGER,
   origin_account TEXT
  )
 `,
 "CREATE INDEX IF NOT EXISTS stacks_txs_confirmed_origin_account ON stacks_txs_confirmed(origin_account)",
 "CREATE INDEX IF NOT EXISTS stacks_txs_confirmed_origin_nonce ON stacks_txs_confirmed(origin_nonce)",
 `CREATE TABLE IF NOT EXISTS stacks_txs_mined_success (
   txid TEXT,
   stacks_height INTEGER,
   fee INTEGER,
   cost_runtime INTEGER,
   cost_write_count INTEGER,
   cost_read_count INTEGER,
   cost_read_len INTEGER,
   cost_write_len INTEGER,
   PRIMARY KEY (txid, stacks_height)
  )
 `,
 "CREATE INDEX IF NOT EXISTS stacks_txs_mined_success_height ON stacks_txs_mined_success(stacks_height)",
 `CREATE TABLE IF NOT EXISTS stacks_txs_mined_failed (
   txid TEXT,
   stacks_height INTEGER,
   status TEXT,
   error TEXT,
   PRIMARY KEY (txid, stacks_height)
  )
 `,
 "CREATE INDEX IF NOT EXISTS stacks_txs_mined_failed_height ON stacks_txs_mined_failed(stacks_height)",
 "CREATE INDEX IF NOT EXISTS stacks_txs_mined_failed_status ON stacks_txs_mined_failed(status)",
 `CREATE TABLE IF NOT EXISTS stacks_txs_received (
   txid TEXT PRIMARY KEY,
   tx_len INTEGER,
   received_height INTEGER,
   origin_nonce INTEGER,
   origin_account TEXT
  )
 `,
 "CREATE INDEX IF NOT EXISTS stacks_txs_received_origin_account ON stacks_txs_received(origin_account)",
 "CREATE INDEX IF NOT EXISTS stacks_txs_received_origin_nonce ON stacks_txs_received(origin_nonce)",
 `CREATE TABLE IF NOT EXISTS burn_blocks (
   block_height INTEGER,
   block_hash TEXT PRIMARY KEY,
   total_spend INTEGER
  )
  `,
 "CREATE INDEX IF NOT EXISTS burn_blocks_block_height ON burn_blocks(block_height)",
];

export class SqliteDataStore {
  db: pg.Client;
  private constructor(
    db: pg.Client,
  ) {
    this.db = db;
  }

  static async open(path: string) {
    const clientConfig = getPgClientConfig();
    let connectionOkay = false;
    const initTimer = stopwatch();
    let lastElapsedLog = 0;
    let connectionError = undefined;
    do {
      const client = new pg.Client(clientConfig);
      try {
        await client.connect();
        connectionOkay = true;
        break;
      } catch (error: any) {
        if (
          error.code !== 'ECONNREFUSED' &&
          error.message !== 'Connection terminated unexpectedly' &&
          !error.message?.includes('database system is starting')
        ) {
          logError('Cannot connect to pg', error);
          throw error;
        }
        const timeElapsed = initTimer.getElapsed();
        if (timeElapsed - lastElapsedLog > 2000) {
          lastElapsedLog = timeElapsed;
          logError('Pg connection failed, retrying..');
        }
        connectionError = error;
        await timeout(100);
      } finally {
        client.end(() => {});
      }
    } while (initTimer.getElapsed() < 10);
    if (!connectionOkay) {
      connectionError = connectionError ?? new Error('Error connecting to database');
      throw connectionError;
    }

    const db = await sqlite.open({
        filename: path,
        driver: sqlite3.Database
    });

    const client = new pg.Client(clientConfig);
    await client.connect();

    return new SqliteDataStore(client)
  }

  async createTables() {
    for (const sql_command of CREATE_TABLES) {
      await this.db.query(sql_command);
    }
  }

  async writeStacksBlock(block: DbBlock) {
    await this.db.query("INSERT INTO stacks_blocks (index_block_hash, anchor_block_hash, parent_index_block_hash, stacks_height, burn_height) VALUES ($1, $2, $3, $4, $5) ON CONFLICT(index_block_hash) DO NOTHING",
                        [block.index_block_hash, block.block_hash, block.parent_index_block_hash, block.block_height, block.burn_block_height]);
  }

  async writeConfirmedTxs(txs: CoreNodeParsedTxMessage[], confirmed_height: number) {
    for (const tx of txs) {
      let cost = tx.core_tx.execution_cost;
      let tx_len = Math.floor(tx.core_tx.raw_tx.length / 2);
      await this.db.query(
         `INSERT INTO stacks_txs_confirmed
              (txid, confirmed_height, origin_nonce, origin_account, fee, tx_len, cost_runtime, cost_write_len, cost_write_count, cost_read_len, cost_read_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT(txid) DO NOTHING`,
        [tx.core_tx.txid, confirmed_height, tx.nonce, tx.sender_address, tx.parsed_tx.auth.originCondition.feeRate, tx_len, cost.runtime, cost.write_length, cost.write_count, cost.read_length, cost.read_count]);
    }
  }

  async writeReceivedTxs(txs: DbMempoolTx[], received_height: number) {
    for (const tx of txs) {
      await this.db.query("INSERT INTO stacks_txs_received (txid, tx_len, received_height, origin_nonce, origin_account) VALUES ($1, $2, $3, $4, $5) ON CONFLICT(txid) DO NOTHING",
                          [tx.tx_id, tx.raw_tx.length, received_height, tx.nonce, tx.sender_address]);
    }
  }

  async writeBurnBlockData(block_height: number, block_hash: string, total_spend: number) {
    await this.db.query("INSERT INTO burn_blocks (block_height, block_hash, total_spend) VALUES ($1, $2, $3) ON CONFLICT(block_hash) DO NOTHING",
                        [block_height, block_hash, total_spend]);
  }

  async writeMinedTransactions(txs: CoreNodeMinedTransactionEvent[], stacks_height: number) {
    for (const tx of txs) {
      if (tx.Success) {
        let execution_cost = tx.Success.execution_cost;
        await this.db.query(
          `INSERT INTO stacks_txs_mined_success
            (txid, stacks_height, fee, cost_runtime, cost_write_count, cost_read_count, cost_write_len, cost_read_len)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT(txid, stacks_height) DO NOTHING`,
          [tx.Success.txid, stacks_height, tx.Success.fee, execution_cost.runtime,
           execution_cost.write_count, execution_cost.read_count, execution_cost.write_length, execution_cost.read_length]
        );
      } else if (tx.TransactionErrorEvent) {
        await this.db.query(
          `INSERT INTO stacks_txs_mined_failed
             (txid, stacks_height, status, error)
           VALUES ($1, $2, $3, $4) ON CONFLICT(txid, stacks_height) DO NOTHING`,
          [tx.TransactionErrorEvent.txid, stacks_height, "TX_ERROR", tx.TransactionErrorEvent.error]
        );
      } else if (tx.TransactionSkippedEvent) {
        await this.db.query(
          `INSERT INTO stacks_txs_mined_failed
             (txid, stacks_height, status, error)
           VALUES ($1, $2, $3, $4) ON CONFLICT(txid, stacks_height) DO NOTHING`,
          [tx.TransactionSkippedEvent.txid, stacks_height, "SKIPPED", tx.TransactionSkippedEvent.error]
        );
      }
    }
  }

  async getConfirmedTransactionsAt(stacks_height: number): Promise<{fee: number, scalar_cost: number}[]> {
    let result = await this.db.query<{fee: number, tx_len: number, cost_runtime: number, cost_write_count: number, cost_write_len: number, cost_read_count: number, cost_read_len: number}>(
      `SELECT (fee, tx_len, cost_runtime, cost_write_count, cost_read_count, cost_write_len, cost_read_len) FROM stacks_txs_confirmed
       WHERE fee > 0 AND confirmed_height = $1`,
      [stacks_height]
    );

    let PROPORTION_RESOLUTION = 10000;

    let limits = {
      write_length: 15000000,
      write_count: 7750,
      read_length: 100000000,
      read_count: 7750,
      runtime: 5000000000,
      block_length: 2097152,
    };

    if (stacks_height > 40606) {
      limits = {
        write_length: 15000000,
        write_count: 15000,
        read_length: 100000000,
        read_count: 15000,
        runtime: 5000000000,
        block_length: 2097152,
      };
    }

    let output = result.rows.map((r) => {
      let scalar_cost =
          PROPORTION_RESOLUTION * ((r.cost_runtime / limits.runtime) +
                                   (r.cost_write_count / limits.write_count) +
                                   (r.cost_read_count / limits.read_count) +
                                   (r.cost_write_len / limits.write_length) +
                                   (r.cost_read_len / limits.read_length) +
                                   (r.tx_len / limits.block_length));
      return { fee: r.fee, scalar_cost };
    });

    return output;
  }

  async getConfirmedTransactionsFromMempool(stacks_height: number): Promise<number[]> {
    let query = `SELECT (confirmed_height - receive_height) as confirmed_within
                 INNER JOIN stacks_txs_received ON stacks_txs_received.txid = stacks_txs_confirmed.txid WHERE confirmed_height = $1`
    let result = await this.db.query<{ confirmed_within: number }>(query, [stacks_height]);

    return result.rows.map((r) => r.confirmed_within);
  }

  async getSuccessMinedTransactionsAt(stacks_height: number): Promise<{fee: number, scalar_cost: number}[]> {
    let result = await this.db.query<{fee: number, tx_len: number, cost_runtime: number, cost_write_count: number, cost_write_len: number, cost_read_count: number, cost_read_len: number}>(
      `SELECT (fee, tx_len, cost_runtime, cost_write_count, cost_read_count, cost_write_len, cost_read_len) FROM stacks_txs_mined_success
       INNER JOIN stacks_txs_received ON stacks_txs_received.txid = stacks_txs_mined_success.txid
       WHERE fee > 0 AND stacks_height = $1`,
      [stacks_height]
    );


    let PROPORTION_RESOLUTION = 10000;

    let limits = {
      write_length: 15000000,
      write_count: 7750,
      read_length: 100000000,
      read_count: 7750,
      runtime: 5000000000,
      block_length: 2097152,
    };

    if (stacks_height > 40606) {
      limits = {
        write_length: 15000000,
        write_count: 15000,
        read_length: 100000000,
        read_count: 15000,
        runtime: 5000000000,
        block_length: 2097152,
      };
    }

    let output = result.rows.map((r) => {
      let scalar_cost =
          PROPORTION_RESOLUTION * ((r.cost_runtime / limits.runtime) +
                                   (r.cost_write_count / limits.write_count) +
                                   (r.cost_read_count / limits.read_count) +
                                   (r.cost_write_len / limits.write_length) +
                                   (r.cost_read_len / limits.read_length) +
                                   (r.tx_len / limits.block_length));
      return { fee: r.fee, scalar_cost };
    });

    return output;
  }

  async countSuccessMinedTransactionsAt(stacks_height: number): Promise<number> {
    let result = await this.db.query<{count: string}>(
      "SELECT COUNT(*) as count FROM stacks_txs_mined_success WHERE fee > 0 AND stacks_height = $1;",
      [stacks_height]
    );
    if (result.rowCount >= 1) {
      return parseInt(result.rows[0].count);
    } else {
      return 0;
    }
  }

  async getFailedMinedTransactionsAt(stacks_height: number): Promise<number> {
    let result = await this.db.query<{count: string}>(
      "SELECT COUNT(*) as count FROM stacks_txs_mined_failed WHERE stacks_height = $1;",
      [stacks_height]
    );
    if (result.rowCount >= 1) {
      return parseInt(result.rows[0].count);
    } else {
      return 0;
    }
  }

  async getBurnBlockHeight(): Promise<number> {
    const result = await this.db.query<{ burn_height: number }>("SELECT MAX(block_height) as burn_height FROM burn_blocks;");
    if (result.rowCount >= 1) {
      return result.rows[0].burn_height;
    } else {
      return 0;
    }
  }

  async getStacksHeight(): Promise<number> {
    const result = await this.db.query<{ stacks_height: number }>("SELECT MAX(stacks_height) as stacks_height FROM stacks_blocks;");
    if (result.rowCount >= 1) {
      return result.rows[0].stacks_height;
    } else {
      return 0;
    }
  }

  async getStacksHeightOfIndexHash(index_hash: string): Promise<number | null> {
    const result = await this.db.query<{ stacks_height: number }>(
      "SELECT stacks_height FROM stacks_blocks WHERE index_block_hash = $1;",
      [index_hash]);
    if (result.rowCount >= 1) {
      return result.rows[0].stacks_height;
    } else {
      return null;
    }
  }

  async getStacksBlocksBetweenBurnHeights(start: number, end: number): Promise<{ index_block_hash: string, parent_index_block_hash: string, burn_height: number }[]> {
    let result = await this.db.query<{ index_block_hash: string, parent_index_block_hash: string, burn_height: number }>(
      "SELECT index_block_hash, parent_index_block_hash, burn_height FROM stacks_blocks WHERE burn_height > $1 AND burn_height <= $2 ORDER BY stacks_height DESC;",
      [start, end]
    );

    return result.rows;
  }

  async getNoBurnCountBetweenBurnHeights(start: number, end: number): Promise<number> {
    let result = await this.db.query<{count: string}>(
      "SELECT COUNT(*) as count FROM burn_blocks WHERE total_spend < 1 AND block_height > $1 and block_height <= $2;",
      [start, end]
    );
    if (result.rowCount >= 1) {
      return parseInt(result.rows[0].count);
    } else {
      return 0;
    }
  }
}
