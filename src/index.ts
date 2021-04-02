import { loadDotEnv, timeout, logger, logError, isProdEnv, numberToHex } from './helpers';
import * as sourceMapSupport from 'source-map-support';
import { DataStore } from './datastore/common';
import { PgDataStore } from './datastore/postgres-store';
import { MemoryDataStore } from './datastore/memory-store';
import { startApiServer } from './api/init';
import { startEventServer } from './event-stream/event-server';
import { StacksCoreRpcClient } from './core-rpc/client';
import { createServer as createPrometheusServer } from '@promster/server';
import { ChainID } from '@stacks/transactions';
import { registerShutdownHandler } from './shutdown-handler';
import { importV1 } from './import-v1';

loadDotEnv();

sourceMapSupport.install({ handleUncaughtExceptions: false });

registerShutdownHandler();

async function monitorCoreRpcConnection(): Promise<void> {
  const CORE_RPC_HEARTBEAT_INTERVAL = 5000; // 5 seconds
  let previouslyConnected = false;
  while (true) {
    const client = new StacksCoreRpcClient();
    try {
      await client.waitForConnection();
      if (!previouslyConnected) {
        logger.info(`Connection to Stacks core node API server at: ${client.endpoint}`);
      }
      previouslyConnected = true;
    } catch (error) {
      previouslyConnected = false;
      logger.error(`Warning: failed to connect to node RPC server at ${client.endpoint}`);
    }
    await timeout(CORE_RPC_HEARTBEAT_INTERVAL);
  }
}

async function getCoreChainID(): Promise<ChainID> {
  const client = new StacksCoreRpcClient();
  await client.waitForConnection(Infinity);
  const coreInfo = await client.getInfo();
  if (coreInfo.network_id === ChainID.Mainnet) {
    return ChainID.Mainnet;
  } else if (coreInfo.network_id === ChainID.Testnet) {
    return ChainID.Testnet;
  } else {
    throw new Error(`Unexpected network_id "${coreInfo.network_id}"`);
  }
}

async function init(): Promise<void> {
  let db: DataStore;
  switch (process.env['STACKS_BLOCKCHAIN_API_DB']) {
    case 'memory': {
      logger.info('using in-memory db');
      db = new MemoryDataStore();
      break;
    }
    case 'pg':
    case undefined: {
      db = await PgDataStore.connect();
      break;
    }
    default: {
      throw new Error(
        `Invalid STACKS_BLOCKCHAIN_API_DB option: "${process.env['STACKS_BLOCKCHAIN_API_DB']}"`
      );
    }
  }

  if (!('STACKS_CHAIN_ID' in process.env)) {
    const error = new Error(`Env var STACKS_CHAIN_ID is not set`);
    logError(error.message, error);
    throw error;
  }
  const configuredChainID: ChainID = parseInt(process.env['STACKS_CHAIN_ID'] as string);

  if (db instanceof PgDataStore) {
    if (isProdEnv && !process.env.BNS_IMPORT_DIR) {
      logger.warn(`Notice: full BNS functionality requires 'BNS_IMPORT_DIR' to be set.`);
    } else if (process.env.BNS_IMPORT_DIR) {
      await importV1(db, process.env.BNS_IMPORT_DIR);
    }
  }

  const eventServer = await startEventServer({ db, chainId: configuredChainID });
  registerShutdownHandler(async () => {
    await new Promise<void>((resolve, reject) => {
      eventServer.close(error => {
        error ? reject(error) : resolve();
      });
    });
  });
  const networkChainId = await getCoreChainID();
  if (networkChainId !== configuredChainID) {
    const chainIdConfig = numberToHex(configuredChainID);
    const chainIdNode = numberToHex(networkChainId);
    const error = new Error(
      `The configured STACKS_CHAIN_ID does not match, configured: ${chainIdConfig}, stacks-node: ${chainIdNode}`
    );
    logError(error.message, error);
    throw error;
  }
  monitorCoreRpcConnection().catch(error => {
    logger.error(`Error monitoring RPC connection: ${error}`, error);
  });
  const apiServer = await startApiServer(db, networkChainId);
  logger.info(`API server listening on: http://${apiServer.address}`);

  if (isProdEnv) {
    await createPrometheusServer({ port: 9153 });
    logger.info(`@promster/server started on port 9153.`);
  }
}

init()
  .then(() => {
    logger.info('App initialized');
  })
  .catch(error => {
    logError(`app failed to start: ${error}`, error);
    process.exit(1);
  });
