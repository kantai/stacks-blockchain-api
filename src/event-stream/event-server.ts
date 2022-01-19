import { inspect } from 'util';
import * as net from 'net';
import { Server, createServer } from 'http';
import * as express from 'express';
import * as bodyParser from 'body-parser';
import { addAsync } from '@awaitjs/express';
import PQueue from 'p-queue';
import * as expressWinston from 'express-winston';
import * as winston from 'winston';

import { hexToBuffer, logError, logger, digestSha512_256, I32_MAX, LogLevel } from '../helpers';
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
} from './core-node-message';
import {
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
  getTxSenderAddress,
  getTxSponsorAddress,
  parseMessageTransaction,
  CoreNodeMsgBlockData,
  parseMicroblocksFromTxs,
} from './reader';
import { TransactionPayloadTypeID, readTransaction } from '../p2p/tx';
import {
  addressToString,
  BufferCV,
  BufferReader,
  ChainID,
  deserializeCV,
  StringAsciiCV,
  TupleCV,
} from '@stacks/transactions';

import { SqliteDataStore } from './event-store';

import * as zoneFileParser from 'zone-file';

const METRICS_WINDOW_SIZE = 4;

async function collectMetrics(db: SqliteDataStore): Promise<void> {
  // 0. Get burn block height
  let burn_height = await db.getBurnBlockHeight();

  logger.info(`burn height: ${burn_height}`);

  if (burn_height < METRICS_WINDOW_SIZE) {
    return;
  }

  let start = burn_height - METRICS_WINDOW_SIZE;
  let stop = burn_height;

  // 1. block orphan rate
  let blocks = await db.getStacksBlocksBetweenBurnHeights(start, stop);

  if (blocks.length < 1) {
    return;
  }

  let blocks_map = new Map<string, { index_block_hash: string, parent_index_block_hash: string, burn_height: number }>();
  for (const block of blocks) {
    blocks_map.set(block.index_block_hash, block);
  }

  // count "good" by traversing from chain tip
  let good_cursor: { index_block_hash: string; parent_index_block_hash: string; burn_height: number } | undefined = blocks[0];
  let good_count = 0;
  while (good_cursor != undefined) {
    good_count += 1;
    good_cursor = blocks_map.get(good_cursor.parent_index_block_hash);
  }

  let block_orphan_percent = 100 * (1 - (good_count / blocks.length));

  // 2. Flash blocks
  let flash_block_count = await db.getNoBurnCountBetweenBurnHeights(start, stop);


  // get stacks height

  let stacks_height = await db.getStacksHeight();

  // 3. Get confirmable tx count at stacks_height + 1

  let confirmable_tx_count = await db.countSuccessMinedTransactionsAt(stacks_height + 1);

  // 4. Get unconfirmable tx count at stacks_height + 1

  let unconfirmable_tx_count = await db.getFailedMinedTransactionsAt(stacks_height + 1);

  // 5. Get considered tx count

  let considered = confirmable_tx_count + unconfirmable_tx_count;

  // 7. Average cost of mock mined transactions

  let fees_costs = await db.getSuccessMinedTransactionsAt(stacks_height + 1);

  let total_fees_costs = fees_costs.reduce((a, b) => {
    return { fee: a.fee + b.fee, scalar_cost: a.scalar_cost + b.scalar_cost }
  }, { fee: 0, scalar_cost: 1 });

  let average_fees = total_fees_costs.fee / fees_costs.length;
  let average_costs = total_fees_costs.scalar_cost / fees_costs.length;

  // 8. Average cost of confirmed transactions

  fees_costs = await db.getConfirmedTransactionsAt(stacks_height);

  total_fees_costs = fees_costs.reduce((a, b) => {
    return { fee: a.fee + b.fee, scalar_cost: a.scalar_cost + b.scalar_cost }
  }, { fee: 0, scalar_cost: 1 });

  let confirmed_average_fees = total_fees_costs.fee / fees_costs.length;
  let confirmed_average_costs = total_fees_costs.scalar_cost / fees_costs.length;

  // 9. Transactions confirmed in block

  let confirmed_tx_count = fees_costs.length;

  // 10. Median transaction confirmation delay over window

  // find the first stacks height that is outside the window
  let block_cursor: { index_block_hash: string; parent_index_block_hash: string; burn_height: number } | undefined = blocks[0];
  while (block_cursor != undefined && block_cursor.burn_height > start) {
    block_cursor = blocks_map.get(block_cursor.parent_index_block_hash);
  }

  if (block_cursor == null) {
    logger.warn("Not enough data yet to evaluate confirmation delay");
    return;
  }

  let stacks_height_start = await db.getStacksHeightOfIndexHash(block_cursor.index_block_hash);
  let stacks_height_end = await db.getStacksHeightOfIndexHash(blocks[0].index_block_hash);

  if (stacks_height_start == null || stacks_height_end == null) {
    logger.error("Could not find stacks block that should have been recorded");
    return;
  }

  let confirmed_tx_delays = [];
  for(let i = stacks_height_start; i <= stacks_height_end; i++){
    const results = await db.getConfirmedTransactionsFromMempool(i);
    results.forEach((tx) => { confirmed_tx_delays.push(tx) });
  }

  logger.info(`orphan rate: ${block_orphan_percent} (${good_count} / ${blocks.length}), flash block count: ${flash_block_count}, confirmable txs: ${confirmable_tx_count}, unconfirmable txs: ${unconfirmable_tx_count}, considered: ${considered}`);
}

async function handleRawEventRequest(
  eventPath: string,
  payload: string,
  db: SqliteDataStore
): Promise<void> {
}

async function handleBurnBlockMessage(
  burnBlockMsg: CoreNodeBurnBlockMessage,
  db: SqliteDataStore
): Promise<void> {
  logger.verbose(
    `Received burn block message hash ${burnBlockMsg.burn_block_hash}, height: ${burnBlockMsg.burn_block_height}, reward recipients: ${burnBlockMsg.reward_recipients.length}`
  );
  const rewards = burnBlockMsg.reward_recipients
        .map(r => r.amt)
        .reduce((a, b) => a + b, 0);

  const burns = burnBlockMsg.burn_amount

  const total_spend = (rewards + burns);

  db.writeBurnBlockData(burnBlockMsg.burn_block_height, burnBlockMsg.burn_block_hash, total_spend);
}

async function handleMempoolTxsMessage(rawTxs: string[], db: SqliteDataStore): Promise<void> {
  logger.verbose(`Received ${rawTxs.length} mempool transactions`);
  // TODO: mempool-tx receipt date should be sent from the core-node
  const receiptDate = Math.round(Date.now() / 1000);
  const rawTxBuffers = rawTxs.map(str => hexToBuffer(str));
  const decodedTxs = rawTxBuffers.map(buffer => {
    const txId = '0x' + digestSha512_256(buffer).toString('hex');
    const bufferReader = BufferReader.fromBuffer(buffer);
    const parsedTx = readTransaction(bufferReader);
    const txSender = getTxSenderAddress(parsedTx);
    const sponsorAddress = getTxSponsorAddress(parsedTx);
    return {
      txId: txId,
      sender: txSender,
      sponsorAddress,
      txData: parsedTx,
      rawTx: buffer,
    };
  });

  const dbMempoolTxs = decodedTxs.map(tx => {
    logger.verbose(`Received mempool tx: ${tx.txId}`);
    const dbMempoolTx = createDbMempoolTxFromCoreMsg({
      txId: tx.txId,
      txData: tx.txData,
      sender: tx.sender,
      sponsorAddress: tx.sponsorAddress,
      rawTx: tx.rawTx,
      receiptDate: receiptDate,
    });
    return dbMempoolTx;
  });

  let received_height = await db.getStacksHeight();
  await db.writeReceivedTxs(dbMempoolTxs, received_height);
}

async function handleDroppedMempoolTxsMessage(
  msg: CoreNodeDropMempoolTxMessage,
  db: SqliteDataStore
): Promise<void> {
  logger.verbose(`Received ${msg.dropped_txids.length} dropped mempool txs`);
  const dbTxStatus = getTxDbStatus(msg.reason);
}

async function handleMicroblockMessage(
  chainId: ChainID,
  msg: CoreNodeMicroblockMessage,
  db: SqliteDataStore
): Promise<void> {
  logger.verbose(`Received microblock with ${msg.transactions.length} txs`);
  const dbMicroblocks = parseMicroblocksFromTxs({
    parentIndexBlockHash: msg.parent_index_block_hash,
    txs: msg.transactions,
    parentBurnBlock: {
      height: msg.burn_block_height,
      hash: msg.burn_block_hash,
      time: msg.burn_block_timestamp,
    },
  });
  const parsedTxs: CoreNodeParsedTxMessage[] = [];
  msg.transactions.forEach(tx => {
    const blockData: CoreNodeMsgBlockData = {
      parent_index_block_hash: msg.parent_index_block_hash,

      parent_burn_block_timestamp: msg.burn_block_timestamp,
      parent_burn_block_height: msg.burn_block_height,
      parent_burn_block_hash: msg.burn_block_hash,

      // These properties aren't known until the next anchor block that accepts this microblock.
      burn_block_time: -1,
      burn_block_height: -1,
      index_block_hash: '',
      block_hash: '',

      // These properties can be determined with a db query, they are set while the db is inserting them.
      block_height: -1,
      parent_block_hash: '',
    };
    const parsedTx = parseMessageTransaction(chainId, tx, blockData, msg.events);
    if (parsedTx) {
      parsedTxs.push(parsedTx);
    }
  });
  parsedTxs.forEach(tx => {
    logger.verbose(`Received microblock mined tx: ${tx.core_tx.txid}`);
  });
}

async function handleMinedBlock(
  chainId: ChainID,
  msg: CoreNodeMinedBlock,
  db: SqliteDataStore
): Promise<void> {
  await db.writeMinedTransactions(msg.tx_events, msg.stacks_height);
}

async function handleBlockMessage(
  chainId: ChainID,
  msg: CoreNodeBlockMessage,
  db: SqliteDataStore
): Promise<void> {
  const parsedTxs: CoreNodeParsedTxMessage[] = [];
  const blockData: CoreNodeMsgBlockData = {
    ...msg,
  };
  msg.transactions.forEach(item => {
    const parsedTx = parseMessageTransaction(chainId, item, blockData, msg.events);
    if (parsedTx) {
      parsedTxs.push(parsedTx);
    }
  });

  const dbBlock: DbBlock = {
    canonical: true,
    block_hash: msg.block_hash,
    index_block_hash: msg.index_block_hash,
    parent_index_block_hash: msg.parent_index_block_hash,
    parent_block_hash: msg.parent_block_hash,
    parent_microblock_hash: msg.parent_microblock,
    parent_microblock_sequence: msg.parent_microblock_sequence,
    block_height: msg.block_height,
    burn_block_time: msg.burn_block_time,
    burn_block_hash: msg.burn_block_hash,
    burn_block_height: msg.burn_block_height,
    miner_txid: msg.miner_txid,
    execution_cost_read_count: 0,
    execution_cost_read_length: 0,
    execution_cost_runtime: 0,
    execution_cost_write_count: 0,
    execution_cost_write_length: 0,
  };

  await db.writeStacksBlock(dbBlock);

  logger.verbose(`Received block ${msg.block_hash} (${msg.block_height}) from node`, dbBlock);

  parsedTxs.forEach(tx => {
    logger.verbose(`Received anchor block mined tx: ${tx.core_tx.txid}`);
  });

  await db.writeConfirmedTxs(parsedTxs, dbBlock.block_height);
}

function parseDataStoreTxEventData(
  parsedTxs: CoreNodeParsedTxMessage[],
  events: CoreNodeEvent[],
  blockData: {
    block_height: number;
    index_block_hash: string;
  }
): DataStoreTxEventData[] {
  const dbData: DataStoreTxEventData[] = parsedTxs.map(tx => {
    const dbTx: DataStoreBlockUpdateData['txs'][number] = {
      tx: createDbTxFromCoreMsg(tx),
      stxEvents: [],
      stxLockEvents: [],
      ftEvents: [],
      nftEvents: [],
      contractLogEvents: [],
      smartContracts: [],
      names: [],
      namespaces: [],
    };
    if (tx.parsed_tx.payload.typeId === TransactionPayloadTypeID.SmartContract) {
      const contractId = `${tx.sender_address}.${tx.parsed_tx.payload.name}`;
      dbTx.smartContracts.push({
        tx_id: tx.core_tx.txid,
        contract_id: contractId,
        block_height: blockData.block_height,
        source_code: tx.parsed_tx.payload.codeBody,
        abi: JSON.stringify(tx.core_tx.contract_abi),
        canonical: true,
      });
    }
    return dbTx;
  });

  for (const event of events) {
    if (!event.committed) {
      logger.verbose(`Ignoring uncommitted tx event from tx ${event.txid}`);
      continue;
    }
    const dbTx = dbData.find(entry => entry.tx.tx_id === event.txid);
    if (!dbTx) {
      throw new Error(`Unexpected missing tx during event parsing by tx_id ${event.txid}`);
    }

    const dbEvent: DbEventBase = {
      event_index: event.event_index,
      tx_id: event.txid,
      tx_index: dbTx.tx.tx_index,
      block_height: blockData.block_height,
      canonical: true,
    };

  }

  // Normalize event indexes from per-block to per-transaction contiguous series.
  for (const tx of dbData) {
    const sortedEvents = [
      tx.contractLogEvents,
      tx.ftEvents,
      tx.nftEvents,
      tx.stxEvents,
      tx.stxLockEvents,
    ]
      .flat()
      .sort((a, b) => a.event_index - b.event_index);
    tx.tx.event_count = sortedEvents.length;
    for (let i = 0; i < sortedEvents.length; i++) {
      sortedEvents[i].event_index = i;
    }
  }

  return dbData;
}

interface EventMessageHandler {
  handleRawEventRequest(eventPath: string, payload: string, db: SqliteDataStore): Promise<void> | void;
  handleBlockMessage(
    chainId: ChainID,
    msg: CoreNodeBlockMessage,
    db: SqliteDataStore
  ): Promise<void> | void;
  handleMicroblockMessage(
    chainId: ChainID,
    msg: CoreNodeMicroblockMessage,
    db: SqliteDataStore
  ): Promise<void> | void;
  handleMempoolTxs(rawTxs: string[], db: SqliteDataStore): Promise<void> | void;
  handleBurnBlock(msg: CoreNodeBurnBlockMessage, db: SqliteDataStore): Promise<void> | void;
  handleDroppedMempoolTxs(msg: CoreNodeDropMempoolTxMessage, db: SqliteDataStore): Promise<void> | void;
  handleMinedBlock(chainId: ChainID, msg: CoreNodeMinedBlock, db: SqliteDataStore): Promise<void> | void;
}

function createMessageProcessorQueue(): EventMessageHandler {
  // Create a promise queue so that only one message is handled at a time.
  const processorQueue = new PQueue({ concurrency: 1 });
  const handler: EventMessageHandler = {
    handleRawEventRequest: (eventPath: string, payload: string, db: SqliteDataStore) => {
      return processorQueue
        .add(() => handleRawEventRequest(eventPath, payload, db))
        .catch(e => {
          logError(`Error storing raw core node request data`, e, payload);
          throw e;
        });
    },
    handleBlockMessage: (chainId: ChainID, msg: CoreNodeBlockMessage, db: SqliteDataStore) => {
      return processorQueue
        .add(() => handleBlockMessage(chainId, msg, db))
        .catch(e => {
          logError(`Error processing core node block message`, e, msg);
          throw e;
        });
    },
    handleMicroblockMessage: (chainId: ChainID, msg: CoreNodeMicroblockMessage, db: SqliteDataStore) => {
      return processorQueue
        .add(() => handleMicroblockMessage(chainId, msg, db))
        .catch(e => {
          logError(`Error processing core node microblock message`, e, msg);
          throw e;
        });
    },
    handleBurnBlock: (msg: CoreNodeBurnBlockMessage, db: SqliteDataStore) => {
      return processorQueue
        .add(() => handleBurnBlockMessage(msg, db))
        .catch(e => {
          logError(`Error processing core node burn block message`, e, msg);
          throw e;
        });
    },
    handleMempoolTxs: (rawTxs: string[], db: SqliteDataStore) => {
      return processorQueue
        .add(() => handleMempoolTxsMessage(rawTxs, db))
        .catch(e => {
          logError(`Error processing core node mempool message`, e, rawTxs);
          throw e;
        });
    },
    handleDroppedMempoolTxs: (msg: CoreNodeDropMempoolTxMessage, db: SqliteDataStore) => {
      return processorQueue
        .add(() => handleDroppedMempoolTxsMessage(msg, db))
        .catch(e => {
          logError(`Error processing core node dropped mempool txs message`, e, msg);
          throw e;
        });
    },
    handleMinedBlock: (chainId: ChainID, msg: CoreNodeMinedBlock, db: SqliteDataStore) => {
      return processorQueue
        .add(() => handleMinedBlock(chainId, msg, db))
        .catch(e => {
          logError(`Error processing core node block message`, e, msg);
          throw e;
        });
    },

  };

  return handler;
}

export type EventStreamServer = net.Server & {
  serverAddress: net.AddressInfo;
  closeAsync: () => Promise<void>;
};

export async function startEventServer(opts: {
  chainId: ChainID;
  messageHandler?: EventMessageHandler;
  /** If not specified, this is read from the STACKS_CORE_EVENT_HOST env var. */
  serverHost?: string;
  /** If not specified, this is read from the STACKS_CORE_EVENT_PORT env var. */
  serverPort?: number;
  httpLogLevel?: LogLevel;
}): Promise<EventStreamServer> {
  const db = await SqliteDataStore.open("/tmp/event-server.sqlite");
  await db.createTables();

  const messageHandler = opts.messageHandler ?? createMessageProcessorQueue();

  let eventHost = opts.serverHost ?? process.env['STACKS_CORE_EVENT_HOST'];
  const eventPort = opts.serverPort ?? parseInt(process.env['STACKS_CORE_EVENT_PORT'] ?? '', 10);
  if (!eventHost) {
    throw new Error(
      `STACKS_CORE_EVENT_HOST must be specified, e.g. "STACKS_CORE_EVENT_HOST=127.0.0.1"`
    );
  }
  if (!Number.isInteger(eventPort)) {
    throw new Error(`STACKS_CORE_EVENT_PORT must be specified, e.g. "STACKS_CORE_EVENT_PORT=3700"`);
  }

  if (eventHost.startsWith('http:')) {
    const { hostname } = new URL(eventHost);
    eventHost = hostname;
  }

  const app = addAsync(express());

  app.use(
    expressWinston.logger({
      format: logger.format,
      transports: logger.transports,
      metaField: (null as unknown) as string,
      statusLevels: {
        error: 'error',
        warn: opts.httpLogLevel ?? 'http',
        success: opts.httpLogLevel ?? 'http',
      },
    })
  );

  app.use(bodyParser.json({ type: 'application/json', limit: '500MB' }));
  app.getAsync('/', (req, res) => {
    res
      .status(200)
      .json({ status: 'ready', msg: 'API event server listening for core-node POST messages' });
  });

  app.postAsync('*', async (req, res) => {
    const eventPath = req.path;
    let payload = JSON.stringify(req.body);
    await messageHandler.handleRawEventRequest(eventPath, payload, db);
    if (logger.isDebugEnabled()) {
      // Skip logging massive event payloads, this _should_ only exclude the genesis block payload which is ~80 MB.
      if (payload.length > 10_000_000) {
        payload = 'payload body too large for logging';
      }
      logger.debug(`[stacks-node event] ${eventPath} ${payload}`);
    }
  });

  app.postAsync('/new_block', async (req, res) => {
    try {
      const msg: CoreNodeBlockMessage = req.body;
      await messageHandler.handleBlockMessage(opts.chainId, msg, db);
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /new_block: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/new_burn_block', async (req, res) => {
    try {
      const msg: CoreNodeBurnBlockMessage = req.body;
      await messageHandler.handleBurnBlock(msg, db);
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /new_burn_block: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/new_mempool_tx', async (req, res) => {
    try {
      const rawTxs: string[] = req.body;
      await messageHandler.handleMempoolTxs(rawTxs, db);
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /new_mempool_tx: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/drop_mempool_tx', async (req, res) => {
    try {
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /drop_mempool_tx: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/attachments/new', async (req, res) => {
    try {
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /attachments/new: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/new_microblocks', async (req, res) => {
    try {
      const msg: CoreNodeMicroblockMessage = req.body;
      await messageHandler.handleMicroblockMessage(opts.chainId, msg, db);
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /new_microblocks: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/mined_block', async (req, res) => {
    try {
      const msg: CoreNodeMinedBlock = req.body;
      logger.verbose("Received mined block event");
      await messageHandler.handleMinedBlock(opts.chainId, msg, db);
      res.status(200).json({ result: 'ok' });
      await collectMetrics(db);
    } catch (error) {
      logError(`error processing core-node /mined_block: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.post('*', (req, res, next) => {
    res.status(404).json({ error: `no route handler for ${req.path}` });
    logError(`Unexpected event on path ${req.path}`);
    next();
  });

  app.use(
    expressWinston.errorLogger({
      winstonInstance: logger as winston.Logger,
      metaField: (null as unknown) as string,
      blacklistedMetaFields: ['trace', 'os', 'process'],
    })
  );

  const server = createServer(app);
  await new Promise<void>((resolve, reject) => {
    server.once('error', error => {
      reject(error);
    });
    server.listen(eventPort, eventHost as string, () => {
      resolve();
    });
  });

  const addr = server.address();
  if (addr === null) {
    throw new Error('server missing address');
  }
  const addrStr = typeof addr === 'string' ? addr : `${addr.address}:${addr.port}`;
  logger.info(`Event observer listening at: http://${addrStr}`);

  const closeFn = async () => {
    await new Promise<void>((resolve, reject) => {
      logger.info('Closing event observer server...');
      server.close(error => (error ? reject(error) : resolve()));
    });
  };
  const eventStreamServer: EventStreamServer = Object.assign(server, {
    serverAddress: addr as net.AddressInfo,
    closeAsync: closeFn,
  });
  return eventStreamServer;
}
