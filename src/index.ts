import {
  loadDotEnv,
  timeout,
  logger,
  logError,
  isProdEnv,
  numberToHex,
  httpPostRequest,
  isReadOnlyMode,
} from './helpers';
import * as sourceMapSupport from 'source-map-support';
import { DataStore } from './datastore/common';
import { cycleMigrations, dangerousDropAllTables, PgDataStore } from './datastore/postgres-store';
import { startEventServer } from './event-stream/event-server';
import { StacksCoreRpcClient } from './core-rpc/client';
import { createServer as createPrometheusServer } from '@promster/server';
import { ChainID } from '@stacks/transactions';
import { registerShutdownConfig } from './shutdown-handler';
import { importV1TokenOfferingData, importV1BnsData } from './import-v1';
import { OfflineDummyStore } from './datastore/offline-dummy-store';
import { Socket } from 'net';
import * as getopts from 'getopts';
import * as fs from 'fs';
import * as path from 'path';
import * as net from 'net';
import { injectC32addressEncodeCache } from './c32-addr-cache';

loadDotEnv();

sourceMapSupport.install({ handleUncaughtExceptions: false });

injectC32addressEncodeCache();

registerShutdownConfig();

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

function getConfiguredChainID() {
  if (!('STACKS_CHAIN_ID' in process.env)) {
    const error = new Error(`Env var STACKS_CHAIN_ID is not set`);
    logError(error.message, error);
    throw error;
  }
  const configuredChainID: ChainID = parseInt(process.env['STACKS_CHAIN_ID'] as string);
  return configuredChainID;
}

async function init(): Promise<void> {
  if (isProdEnv && !fs.existsSync('.git-info')) {
    throw new Error(
      'File not found: .git-info. This generated file is required to display the running API version in the ' +
        '`/extended/v1/status` endpoint. Please execute `npm run build` to regenerate it.'
    );
  }

  let configuredChainID = getConfiguredChainID();

  const eventServer = await startEventServer({
    // datastore: db,
    chainId: configuredChainID,
  });
  registerShutdownConfig({
    name: 'Event Server',
    handler: () => eventServer.closeAsync(),
    forceKillable: false,
  });

  if (isProdEnv) {
    const prometheusServer = await createPrometheusServer({ port: 9153 });
    logger.info(`@promster/server started on port 9153.`);
    const sockets = new Set<Socket>();
    prometheusServer.on('connection', socket => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
    });
    registerShutdownConfig({
      name: 'Prometheus',
      handler: async () => {
        for (const socket of sockets) {
          socket.destroy();
          sockets.delete(socket);
        }
        await Promise.resolve(prometheusServer.close());
      },
      forceKillable: true,
    });
  }
}

function initApp() {
  init()
    .then(() => {
      logger.info('App initialized');
    })
    .catch(error => {
      logError(`app failed to start: ${error}`, error);
      process.exit(1);
    });
}

async function handleProgramArgs() {
  logger.level = 'info';
  initApp();
}

void handleProgramArgs().catch(error => {
  console.error(error);
  process.exit(1);
});
