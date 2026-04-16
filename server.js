require('dotenv').config();
const express = require('express');
const path = require('path');
const { ethers } = require('ethers');

const fs = require('fs');
const app = express();
app.use(express.json());
const PORT = parseInt(process.env.PORT || '5179');

// --- Config (from .env) ---
const WALLETS_FILE = path.join(__dirname, 'wallets.json');
const ENV_WALLETS = JSON.parse(process.env.WALLETS || '[]');

// Dynamic wallet list: load from wallets.json, fallback to .env
function loadWallets() {
  try {
    if (fs.existsSync(WALLETS_FILE)) {
      return JSON.parse(fs.readFileSync(WALLETS_FILE, 'utf8'));
    }
  } catch (e) {
    console.error('Failed to load wallets.json:', e.message);
  }
  // Initialize from .env
  saveWallets(ENV_WALLETS);
  return [...ENV_WALLETS];
}

function saveWallets(wallets) {
  fs.writeFileSync(WALLETS_FILE, JSON.stringify(wallets, null, 2), 'utf8');
}

let WALLETS = loadWallets();
const BSC_RPC = process.env.BSC_RPC || 'https://bsc-dataseed.binance.org';
const ANKR_ADVANCED_URL = process.env.ANKR_ADVANCED_URL || (process.env.BSC_RPC?.includes('ankr.com') ? process.env.BSC_RPC.replace('/bsc/', '/multichain/') : '');
// V3
const V3_POSITION_MANAGER = '0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613';
const V3_FACTORY = '0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7';
const WBNB = '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c';
// V4
const V4_POSITION_MANAGER = '0x7a4a5c919ae2541aed11041a1aeee68f1287f95b';
const V4_STATE_VIEW = '0xd13dd3d6e93f276fafc9db9e6bb47c1180aee0c4';
const V4_POOL_MANAGER = '0x28e2ea090877bf75740558f6bfb36a5ffee9e9df';
// The Graph key pool (round-robin + circuit breaker)
const GRAPH_KEYS = (process.env.GRAPH_KEYS || '').split(',').filter(Boolean).map(key => ({
  key: key.trim(), ok: 0, fail: 0, blockedUntil: 0,
}));
let graphKeyIndex = 0;
const GRAPH_BLOCK_MS = 10 * 60 * 1000; // 10 min circuit breaker
const V4_SUBGRAPH_ID = 'EAq1nJKgjnuKH6Gj4RFjCW7LcL7E2uipbncdwV7TTWkX';
const V3_SUBGRAPH_ID = 'F85MNzUGYqgSHSHRGgeVMNsdnW1KtZSVgFULumXRZTw2';

function getNextGraphKey() {
  const now = Date.now();
  for (let i = 0; i < GRAPH_KEYS.length; i++) {
    const idx = (graphKeyIndex + i) % GRAPH_KEYS.length;
    if (GRAPH_KEYS[idx].blockedUntil <= now) {
      graphKeyIndex = (idx + 1) % GRAPH_KEYS.length;
      return GRAPH_KEYS[idx];
    }
  }
  // All blocked — use the one that unblocks soonest
  const sorted = [...GRAPH_KEYS].sort((a, b) => a.blockedUntil - b.blockedUntil);
  return sorted[0];
}

function graphUrl(subgraphId) {
  const entry = getNextGraphKey();
  return { url: `https://gateway.thegraph.com/api/${entry.key}/subgraphs/id/${subgraphId}`, entry };
}

async function graphQuery(subgraphId, query) {
  const { url, entry } = graphUrl(subgraphId);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
    if (!res.ok) {
      entry.fail++;
      if (res.status === 429) entry.blockedUntil = Date.now() + GRAPH_BLOCK_MS;
      return null;
    }
    const json = await res.json();
    if (json.errors?.length) { entry.fail++; return null; }
    entry.ok++;
    return json.data;
  } catch (e) {
    entry.fail++;
    return null;
  }
}

// V4 subgraph position ID cache (per wallet, 6h TTL)
const v4IdCache = {};
const V4_ID_CACHE_TTL = 10 * 60 * 1000; // 10 min, match main cache

// --- Concurrency control ---
const MAX_CONCURRENT = 2;
const BATCH_SIZE = 5; // max parallel RPC calls per batch
const BATCH_DELAY = 300; // ms between batches

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Run promises in small batches with delay between
async function batchedAll(items, fn, batchSize = BATCH_SIZE) {
  const results = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.all(batch.map(fn));
    results.push(...batchResults);
    if (i + batchSize < items.length) await sleep(BATCH_DELAY);
  }
  return results;
}

// Retry wrapper
async function withRetry(fn, retries = 3, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    try { return await fn(); }
    catch (e) {
      if (i === retries - 1) throw e;
      console.log(`  Retry ${i + 1}/${retries} after error: ${e.message.slice(0, 80)}`);
      await sleep(delayMs * (i + 1));
    }
  }
}

// --- Cache ---
let cache = { data: null, timestamp: 0 };
const CACHE_TTL = 10 * 60 * 1000; // 10 min auto-refresh

// --- ABIs ---
// V3
const V3_POSITION_MANAGER_ABI = [
  'function balanceOf(address owner) view returns (uint256)',
  'function tokenOfOwnerByIndex(address owner, uint256 index) view returns (uint256)',
  'function positions(uint256 tokenId) view returns (uint96 nonce, address operator, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1)',
  'function collect(tuple(uint256 tokenId, address recipient, uint128 amount0Max, uint128 amount1Max) params) returns (uint256 amount0, uint256 amount1)',
];
// V4
const V4_POSITION_MANAGER_ABI = [
  'function getPoolAndPositionInfo(uint256 tokenId) view returns (tuple(address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) poolKey, uint256 info)',
  'function getPositionLiquidity(uint256 tokenId) view returns (uint128 liquidity)',
];
const V4_STATE_VIEW_ABI = [
  'function getSlot0(bytes32 poolId) view returns (uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)',
  'function getPositionInfo(bytes32 poolId, address owner, int24 tickLower, int24 tickUpper, bytes32 salt) view returns (uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128)',
  'function getFeeGrowthInside(bytes32 poolId, int24 tickLower, int24 tickUpper) view returns (uint256 feeGrowthInside0X128, uint256 feeGrowthInside1X128)',
];
const Q128 = 2n ** 128n;

const ERC20_ABI = [
  'function symbol() view returns (string)',
  'function decimals() view returns (uint8)',
];

const POOL_ABI = [
  'function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)',
  'function token0() view returns (address)',
  'function token1() view returns (address)',
];

const FACTORY_ABI = [
  'function getPool(address tokenA, address tokenB, uint24 fee) view returns (address)',
];

// --- Providers ---
const provider = new ethers.JsonRpcProvider(BSC_RPC);
// Separate provider for log queries (public RPCs have different rate limits)
const LOG_RPC = process.env.LOG_RPC || 'https://bsc-rpc.publicnode.com';
const logProvider = new ethers.JsonRpcProvider(LOG_RPC);

// --- Token info cache ---
const tokenInfoCache = {};

async function getTokenInfo(address) {
  const addr = address.toLowerCase();
  const cached = tokenInfoCache[addr];
  // Return cached if it's a real symbol (not a fallback address stub)
  if (cached && !cached._fallback) return cached;
  // If fallback is older than 10 min, retry
  if (cached && cached._fallback && (Date.now() - cached._ts < 10 * 60 * 1000)) return cached;
  const contract = new ethers.Contract(address, ERC20_ABI, provider);
  try {
    const [symbol, decimals] = await Promise.all([
      contract.symbol(),
      contract.decimals(),
    ]);
    tokenInfoCache[addr] = { symbol, decimals: Number(decimals), address };
    return tokenInfoCache[addr];
  } catch (e) {
    const fallback = { symbol: addr.slice(0, 6) + '...', decimals: 18, address, _fallback: true, _ts: Date.now() };
    tokenInfoCache[addr] = fallback;
    return fallback;
  }
}

// --- Math helpers ---
function tickToPrice(tick, decimals0, decimals1) {
  return Math.pow(1.0001, tick) * Math.pow(10, decimals0 - decimals1);
}

function sqrtPriceX96ToPrice(sqrtPriceX96, decimals0, decimals1) {
  const sqrtPrice = Number(sqrtPriceX96) / Math.pow(2, 96);
  return sqrtPrice * sqrtPrice * Math.pow(10, decimals0 - decimals1);
}

function getTokenAmounts(liquidity, sqrtPriceX96, tickLower, tickUpper, decimals0, decimals1) {
  const liq = Number(liquidity);
  if (liq === 0) return { amount0: 0, amount1: 0 };

  const sqrtPrice = Number(sqrtPriceX96) / Math.pow(2, 96);
  const sqrtPriceLower = Math.pow(1.0001, tickLower / 2);
  const sqrtPriceUpper = Math.pow(1.0001, tickUpper / 2);

  let amount0 = 0;
  let amount1 = 0;

  if (sqrtPrice <= sqrtPriceLower) {
    amount0 = liq * (1 / sqrtPriceLower - 1 / sqrtPriceUpper);
  } else if (sqrtPrice >= sqrtPriceUpper) {
    amount1 = liq * (sqrtPriceUpper - sqrtPriceLower);
  } else {
    amount0 = liq * (1 / sqrtPrice - 1 / sqrtPriceUpper);
    amount1 = liq * (sqrtPrice - sqrtPriceLower);
  }

  amount0 = amount0 / Math.pow(10, decimals0);
  amount1 = amount1 / Math.pow(10, decimals1);

  return { amount0, amount1 };
}

// --- USD Price Resolution ---
const USDT_ADDRESS = '0x55d398326f99059fF775485246999027B3197955'.toLowerCase();
const BUSD_ADDRESS = '0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56'.toLowerCase();
const STABLECOINS = new Set([USDT_ADDRESS, BUSD_ADDRESS]);

async function getUSDPrices(tokenAddresses, positionsData) {
  const unique = [...new Set(tokenAddresses.map(a => a.toLowerCase()))];
  const prices = {};

  for (const addr of unique) {
    if (STABLECOINS.has(addr)) prices[addr] = 1.0;
  }

  const needCoinGecko = unique.filter(a => !prices[a]);
  if (needCoinGecko.length > 0) {
    try {
      const addresses = needCoinGecko.join(',');
      const url = `https://api.coingecko.com/api/v3/simple/token_price/binance-smart-chain?contract_addresses=${addresses}&vs_currencies=usd`;
      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        for (const [addr, info] of Object.entries(data)) {
          if (info.usd) prices[addr.toLowerCase()] = info.usd;
        }
      }
    } catch (e) {
      console.error('CoinGecko API error:', e.message);
    }
  }

  // Derive prices from pool data, preferring active in-range positions
  // Track which source set each price so we can upgrade later
  const priceSource = {}; // addr -> 'active-inrange' | 'active' | 'inactive'
  for (const pos of positionsData) {
    const t0 = pos.token0addr.toLowerCase();
    const t1 = pos.token1addr.toLowerCase();
    if (pos.currentPrice <= 0) continue;

    let target, price;
    if (STABLECOINS.has(t1) && !STABLECOINS.has(t0)) {
      target = t0; price = pos.currentPrice;
    } else if (STABLECOINS.has(t0) && !STABLECOINS.has(t1)) {
      target = t1; price = 1 / pos.currentPrice;
    } else continue;

    const src = pos.liquidityActive && pos.inRange ? 'active-inrange'
              : pos.liquidityActive ? 'active'
              : 'inactive';
    const rank = { 'active-inrange': 3, 'active': 2, 'inactive': 1 };
    const existing = priceSource[target];
    if (!prices[target] || rank[src] > rank[existing]) {
      prices[target] = price;
      priceSource[target] = src;
    }
  }

  for (const addr of unique) {
    if (!prices[addr]) prices[addr] = 0;
  }

  return prices;
}

// --- Uncollected fees via collect staticCall ---
async function getUnclaimedFees(positionManager, tokenId, walletAddress) {
  try {
    const MAX_UINT128 = BigInt('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF');
    const result = await positionManager.collect.staticCall({
      tokenId: tokenId,
      recipient: walletAddress,
      amount0Max: MAX_UINT128,
      amount1Max: MAX_UINT128,
    }, { from: walletAddress });
    return { fees0: result.amount0, fees1: result.amount1 };
  } catch (e) {
    return { fees0: 0n, fees1: 0n };
  }
}

// --- Fee tier labels ---
function feeLabel(fee) {
  const map = { 100: '0.01%', 500: '0.05%', 2500: '0.25%', 3000: '0.3%', 10000: '1%' };
  return map[Number(fee)] || `${(Number(fee) / 10000).toFixed(2)}%`;
}

// --- V4 helpers ---
function decodePackedPositionInfo(info) {
  const tickUpperRaw = Number((info >> 32n) & 0xffffffn);
  const tickLowerRaw = Number((info >> 8n) & 0xffffffn);
  return {
    tickUpper: tickUpperRaw >= 0x800000 ? tickUpperRaw - 0x1000000 : tickUpperRaw,
    tickLower: tickLowerRaw >= 0x800000 ? tickLowerRaw - 0x1000000 : tickLowerRaw,
  };
}

function computePoolId(poolKey) {
  return ethers.keccak256(
    ethers.AbiCoder.defaultAbiCoder().encode(
      ['address', 'address', 'uint24', 'int24', 'address'],
      [poolKey.currency0, poolKey.currency1, poolKey.fee, poolKey.tickSpacing, poolKey.hooks]
    )
  );
}

// V4: query subgraph for position IDs (cached 6h, key-pool)
async function getV4PositionIds(walletAddress) {
  const cacheKey = walletAddress.toLowerCase();
  const cached = v4IdCache[cacheKey];
  if (cached && (Date.now() - cached.ts < V4_ID_CACHE_TTL)) {
    return cached.ids;
  }
  const query = `{ positions(first: 200, where: { owner: "${cacheKey}" }) { tokenId createdAtTimestamp } }`;
  const data = await graphQuery(V4_SUBGRAPH_ID, query);
  if (!data?.positions) {
    // Return stale cache if available
    return cached?.ids || [];
  }
  const ids = data.positions.map(p => ({ id: BigInt(p.tokenId), createdAt: Number(p.createdAtTimestamp || 0) * 1000 }));
  v4IdCache[cacheKey] = { ids, ts: Date.now() };
  return ids;
}

// V4: get token info, handling native BNB (address(0))
async function getTokenInfoV4(address) {
  if (address === ethers.ZeroAddress || address === '0x0000000000000000000000000000000000000000') {
    return { symbol: 'BNB', decimals: 18, address: WBNB }; // use WBNB address for pricing
  }
  return getTokenInfo(address);
}

// V3: query subgraph for position creation timestamps + collected fees history
async function getV3SubgraphData(walletAddress) {
  const addr = walletAddress.toLowerCase();
  const query = `{ positions(first: 200, where: { owner: "${addr}" }) { id transaction { timestamp } collectedFeesToken0 collectedFeesToken1 } }`;
  try {
    const data = await graphQuery(V3_SUBGRAPH_ID, query);
    if (!data?.positions || data.positions.length === 0) {
      const chainCreated = await getV3CreatedFromChain(walletAddress);
      return { created: chainCreated, collectedFees: {} };
    }
    const created = {};
    const collectedFees = {};
    for (const p of data.positions) {
      const ts = p.transaction?.timestamp;
      if (ts) created[p.id] = Number(ts) * 1000;
      collectedFees[p.id] = {
        token0: parseFloat(p.collectedFeesToken0 || '0'),
        token1: parseFloat(p.collectedFeesToken1 || '0'),
      };
    }
    if (Object.keys(created).length === 0) {
      const chainCreated = await getV3CreatedFromChain(walletAddress);
      return { created: chainCreated, collectedFees };
    }
    return { created, collectedFees };
  } catch (e) {
    console.error(`V3 subgraph query failed for ${addr}:`, e.message?.slice(0, 80));
    const chainCreated = await getV3CreatedFromChain(walletAddress);
    return { created: chainCreated, collectedFees: {} };
  }
}

// V3: get last Collect event timestamps from chain for specific tokenIds
const v3CollectCache = {};
const V3_COLLECT_CACHE_TTL = 30 * 60 * 1000; // 30 min
const COLLECT_EVENT_TOPIC = '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01';

async function getV3LastCollectTimes(tokenIds) {
  const results = {};
  const toQuery = [];
  const now = Date.now();

  for (const tid of tokenIds) {
    const key = tid.toString();
    const cached = v3CollectCache[key];
    if (cached && (now - cached.ts < V3_COLLECT_CACHE_TTL)) {
      if (cached.collectAt) results[key] = cached.collectAt;
    } else {
      toQuery.push(tid);
    }
  }

  if (toQuery.length === 0) return results;

  // Strategy 1: Ankr Advanced API (one request per token, ~200ms each)
  if (ANKR_ADVANCED_URL) {
    const remaining = [];
    for (const tid of toQuery) {
      const key = tid.toString();
      try {
        const tokenIdHex = ethers.zeroPadValue(ethers.toBeHex(tid), 32);
        const body = {
          jsonrpc: '2.0', method: 'ankr_getLogs', id: 1,
          params: {
            blockchain: 'bsc',
            address: [V3_POSITION_MANAGER],
            topics: [[COLLECT_EVENT_TOPIC], [tokenIdHex]],
            fromBlock: 1, toBlock: 'latest',
            pageSize: 1, descOrder: true
          }
        };
        const res = await fetch(ANKR_ADVANCED_URL, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body), signal: AbortSignal.timeout(10000)
        });
        const data = await res.json();
        if (data.result?.logs?.[0]) {
          const log = data.result.logs[0];
          const time = parseInt(log.timestamp, 16) * 1000;
          results[key] = time;
          v3CollectCache[key] = { collectAt: time, ts: now };
          console.log(`  Last collect for #${key}: ${new Date(time).toISOString()} (Ankr Advanced)`);
        } else {
          v3CollectCache[key] = { collectAt: 0, ts: now };
        }
      } catch (e) {
        console.log(`  Ankr collect failed for #${key}: ${e.message?.slice(0, 80)}, queuing fallback`);
        remaining.push(tid);
      }
    }
    if (remaining.length === 0) return results;
    toQuery.length = 0;
    toQuery.push(...remaining);
  }

  // Strategy 2: Fallback to chunked getLogs scan (publicnode)
  try {
    const currentBlock = await logProvider.getBlockNumber();
    const blockStep = 5000;
    const lookbackBlocks = 864000; // ~30 days
    const fromBlock = Math.max(0, currentBlock - lookbackBlocks);

    for (const tid of toQuery) {
      const key = tid.toString();
      try {
        const tokenIdHex = ethers.zeroPadValue(ethers.toBeHex(tid), 32);
        let foundBlockNumber = null;

        for (let end = currentBlock; end >= fromBlock; end -= blockStep) {
          const start = Math.max(fromBlock, end - blockStep + 1);
          try {
            const logs = await logProvider.getLogs({
              address: V3_POSITION_MANAGER,
              topics: [COLLECT_EVENT_TOPIC, tokenIdHex],
              fromBlock: start,
              toBlock: end,
            });
            if (logs.length > 0) {
              foundBlockNumber = logs[logs.length - 1].blockNumber;
              break;
            }
          } catch (e) {
            await sleep(500);
          }
          await sleep(50);
        }

        if (foundBlockNumber) {
          const block = await logProvider.getBlock(foundBlockNumber);
          if (block) {
            results[key] = block.timestamp * 1000;
            v3CollectCache[key] = { collectAt: block.timestamp * 1000, ts: now };
          }
        } else {
          v3CollectCache[key] = { collectAt: 0, ts: now };
        }
      } catch (e) {
        console.error(`  Collect event query failed for token ${key}:`, e.message?.slice(0, 120));
        v3CollectCache[key] = { collectAt: 0, ts: now };
      }
    }
  } catch (e) {
    console.error('Collect event batch query failed:', e.message?.slice(0, 120));
  }

  return results;
}

// V4: get last Collect timestamps from PoolManager ModifyLiquidity events (liquidityDelta=0)
const v4CollectCache = {};
const V4_COLLECT_CACHE_TTL = 30 * 60 * 1000;
const MODIFY_LIQUIDITY_TOPIC = '0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec';

async function getV4LastCollectTimes(positions) {
  // positions: array of { tokenId, poolId, tickLower, tickUpper }
  const results = {};
  const toQuery = [];
  const now = Date.now();

  for (const pos of positions) {
    const key = pos.tokenId.toString();
    const cached = v4CollectCache[key];
    if (cached && (now - cached.ts < V4_COLLECT_CACHE_TTL)) {
      if (cached.collectAt) results[key] = cached.collectAt;
    } else {
      toQuery.push(pos);
    }
  }

  if (toQuery.length === 0) return results;

  // Group by poolId to minimize queries
  const byPool = new Map();
  for (const pos of toQuery) {
    const arr = byPool.get(pos.poolId) || [];
    arr.push(pos);
    byPool.set(pos.poolId, arr);
  }

  const V4_PM_PADDED = ethers.zeroPadValue(V4_POSITION_MANAGER, 32);

  for (const [poolId, poolPositions] of byPool) {
    try {
      // Query all ModifyLiquidity events for this pool from PositionManager
      const body = {
        jsonrpc: '2.0', method: 'ankr_getLogs', id: 1,
        params: {
          blockchain: 'bsc',
          address: [V4_POOL_MANAGER],
          topics: [[MODIFY_LIQUIDITY_TOPIC], [poolId], [V4_PM_PADDED]],
          fromBlock: 1, toBlock: 'latest',
          pageSize: 100, descOrder: true,
        },
      };

      const ankrUrl = ANKR_ADVANCED_URL || BSC_RPC.replace('/bsc/', '/multichain/');
      const res = await fetch(ankrUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(15000),
      });
      const data = await res.json();
      const logs = data.result?.logs || [];


      for (const pos of poolPositions) {
        const key = pos.tokenId.toString();
        let found = false;

        for (const log of logs) {
          try {
            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(
              ['int24', 'int24', 'int256', 'bytes32'], log.data
            );
            const tickLower = Number(decoded[0]);
            const tickUpper = Number(decoded[1]);
            const liquidityDelta = decoded[2];

            // Match: same tick range AND liquidityDelta === 0 (pure collect)
            if (tickLower === pos.tickLower && tickUpper === pos.tickUpper && liquidityDelta === 0n) {
              const time = parseInt(log.timestamp, 16) * 1000;
              results[key] = time;
              v4CollectCache[key] = { collectAt: time, ts: now };
              console.log(`  V4 last collect for #${key}: ${new Date(time).toISOString()} (ticks ${tickLower}~${tickUpper})`);
              found = true;
              break;
            }
          } catch (e) { /* skip malformed log */ }
        }

        if (!found) {
          v4CollectCache[key] = { collectAt: 0, ts: now };
        }
      }
    } catch (e) {
      console.error(`  V4 collect query failed for pool ${poolId.slice(0, 20)}:`, e.message?.slice(0, 100));
      for (const pos of poolPositions) {
        v4CollectCache[pos.tokenId.toString()] = { collectAt: 0, ts: now };
      }
    }
  }

  return results;
}

// V3: fallback - get creation time from NFT mint Transfer event for a SINGLE tokenId
const v3CreatedChainCache = {};
const V3_CREATED_CHAIN_TTL = 60 * 60 * 1000; // 1h
const TRANSFER_EVENT_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const ZERO_ADDR_PADDED = ethers.zeroPadValue('0x0000000000000000000000000000000000000000', 32);

async function getV3CreatedFromChain(walletAddress) {
  // Returns empty map — real per-tokenId lookup happens in getV3MintTime
  return {};
}

// Per-tokenId mint time lookup (chunked, backward scan from latest)
async function getV3MintTime(tokenId) {
  const key = tokenId.toString();
  const cached = v3CreatedChainCache[key];
  if (cached && (Date.now() - cached.ts < V3_CREATED_CHAIN_TTL)) return cached.time;

  // Strategy 1: Ankr Advanced API (single request, ~200ms)
  if (ANKR_ADVANCED_URL) {
    try {
      const body = {
        jsonrpc: '2.0', method: 'ankr_getLogs', id: 1,
        params: {
          blockchain: 'bsc',
          address: [V3_POSITION_MANAGER],
          topics: [
            [TRANSFER_EVENT_TOPIC],
            [ZERO_ADDR_PADDED],
            [],
            [ethers.zeroPadValue(ethers.toBeHex(tokenId), 32)]
          ],
          fromBlock: 1, toBlock: 'latest', pageSize: 1, descOrder: false
        }
      };
      const res = await fetch(ANKR_ADVANCED_URL, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body), signal: AbortSignal.timeout(10000)
      });
      const data = await res.json();
      if (data.result?.logs?.[0]) {
        const log = data.result.logs[0];
        const time = parseInt(log.timestamp, 16) * 1000;
        v3CreatedChainCache[key] = { time, ts: Date.now() };
        console.log(`  Mint time for #${key}: ${new Date(time).toISOString()} (Ankr Advanced)`);
        return time;
      }
    } catch (e) {
      console.log(`  Ankr Advanced mint time failed for #${key}: ${e.message?.slice(0, 80)}, falling back to chain scan`);
    }
  }

  // Strategy 2: Fallback to chunked getLogs scan
  try {
    const currentBlock = await logProvider.getBlockNumber();
    const blockStep = 5000;
    const lookback = 864000; // ~30 days
    const fromBlock = Math.max(0, currentBlock - lookback);
    const tokenIdHex = ethers.zeroPadValue(ethers.toBeHex(tokenId), 32);

    for (let end = currentBlock; end >= fromBlock; end -= blockStep) {
      const start = Math.max(fromBlock, end - blockStep + 1);
      try {
        const logs = await logProvider.getLogs({
          address: V3_POSITION_MANAGER,
          topics: [TRANSFER_EVENT_TOPIC, ZERO_ADDR_PADDED, null, tokenIdHex],
          fromBlock: start,
          toBlock: end,
        });
        if (logs.length > 0) {
          const block = await logProvider.getBlock(logs[0].blockNumber);
          const time = block ? block.timestamp * 1000 : 0;
          v3CreatedChainCache[key] = { time, ts: Date.now() };
          console.log(`  Mint time for #${key}: ${new Date(time).toISOString()} (chain scan)`);
          return time;
        }
      } catch (e) {
        await sleep(500);
      }
      await sleep(50);
    }
  } catch (e) {
    console.error(`  Mint time query failed for #${key}:`, e.message?.slice(0, 120));
  }

  v3CreatedChainCache[key] = { time: 0, ts: Date.now() };
  return 0;
}

// --- Concurrency limiter ---
async function asyncPool(limit, items, fn) {
  const results = [];
  const executing = new Set();
  for (const item of items) {
    const p = Promise.resolve().then(() => fn(item));
    results.push(p);
    executing.add(p);
    const clean = () => executing.delete(p);
    p.then(clean, clean);
    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.all(results);
}

// --- Fetch V4 positions for a single wallet ---
async function fetchWalletV4Positions(wallet, v4pm, stateView) {
  const walletAddress = wallet.address;
  const walletName = wallet.name;

  const tokenIds = await getV4PositionIds(walletAddress);
  if (tokenIds.length === 0) return [];

  console.log(`  ${walletName} V4: ${tokenIds.length} positions from subgraph`);
  const positions = [];

  for (const tokenEntry of tokenIds) {
    const tokenId = typeof tokenEntry === 'object' ? tokenEntry.id : tokenEntry;
    const createdAtMs = typeof tokenEntry === 'object' ? tokenEntry.createdAt : 0;
    try {
      const [[poolKey, info], liquidity] = await Promise.all([
        withRetry(() => v4pm.getPoolAndPositionInfo(tokenId)),
        withRetry(() => v4pm.getPositionLiquidity(tokenId)),
      ]);

      const { tickLower, tickUpper } = decodePackedPositionInfo(info);
      const token0Info = await getTokenInfoV4(poolKey.currency0);
      const token1Info = await getTokenInfoV4(poolKey.currency1);

      // Get pool state
      const poolId = computePoolId(poolKey);
      let sqrtPriceX96, currentTick;
      try {
        const slot0 = await withRetry(() => stateView.getSlot0(poolId));
        sqrtPriceX96 = slot0.sqrtPriceX96;
        currentTick = Number(slot0.tick);
      } catch (e) {
        console.error(`  V4 StateView error for token ${tokenId}:`, e.message.slice(0, 80));
        continue;
      }

      const inRange = currentTick >= tickLower && currentTick < tickUpper;
      const currentPrice = sqrtPriceX96ToPrice(sqrtPriceX96, token0Info.decimals, token1Info.decimals);
      const lowerPrice = tickToPrice(tickLower, token0Info.decimals, token1Info.decimals);
      const upperPrice = tickToPrice(tickUpper, token0Info.decimals, token1Info.decimals);

      const { amount0, amount1 } = getTokenAmounts(
        liquidity, sqrtPriceX96, tickLower, tickUpper,
        token0Info.decimals, token1Info.decimals
      );

      // V4 uncollected fees calculation
      let feesOwed0 = 0, feesOwed1 = 0;
      if (Number(liquidity) > 0) {
        try {
          const salt = ethers.zeroPadValue(ethers.toBeHex(tokenId), 32);
          const [posInfo, fgi] = await Promise.all([
            withRetry(() => stateView.getPositionInfo(poolId, V4_POSITION_MANAGER, tickLower, tickUpper, salt)),
            withRetry(() => stateView.getFeeGrowthInside(poolId, tickLower, tickUpper)),
          ]);
          const posLiq = posInfo.liquidity;
          if (posLiq > 0n) {
            // uint256 wrapping subtraction (Solidity semantics)
            const MAX_U256 = (1n << 256n) - 1n;
            const diff0 = (fgi.feeGrowthInside0X128 - posInfo.feeGrowthInside0LastX128 + MAX_U256 + 1n) & MAX_U256;
            const diff1 = (fgi.feeGrowthInside1X128 - posInfo.feeGrowthInside1LastX128 + MAX_U256 + 1n) & MAX_U256;
            const raw0 = diff0 * posLiq / Q128;
            const raw1 = diff1 * posLiq / Q128;
            feesOwed0 = Number(raw0) / Math.pow(10, token0Info.decimals);
            feesOwed1 = Number(raw1) / Math.pow(10, token1Info.decimals);
            // Sanity check: if fees are unreasonably large, likely stale data
            if (feesOwed0 > 1e12) feesOwed0 = 0;
            if (feesOwed1 > 1e12) feesOwed1 = 0;
          }
        } catch (e) {
          console.error(`  V4 fee calc error for token ${tokenId}:`, e.message.slice(0, 80));
        }
      }

      positions.push({
        tokenId: tokenId.toString(),
        token0: token0Info,
        token1: token1Info,
        token0addr: token0Info.address,
        token1addr: token1Info.address,
        fee: Number(poolKey.fee),
        feeLabel: feeLabel(poolKey.fee),
        tickLower,
        tickUpper,
        currentTick,
        liquidity: liquidity.toString(),
        liquidityActive: Number(liquidity) > 0,
        inRange,
        currentPrice,
        lowerPrice,
        upperPrice,
        amount0,
        amount1,
        feesOwed0,
        feesOwed1,
        poolAddress: poolId,
        walletName,
        walletAddress,
        protocol: 'V4',
        createdAt: createdAtMs || 0,
        lastCollectAt: 0, // filled later by getV4LastCollectTimes
      });
    } catch (e) {
      console.error(`  V4 error for token ${tokenId}:`, e.message.slice(0, 80));
    }
    await sleep(150);
  }
  return positions;
}

// --- Fetch V3 positions for a single wallet ---
async function fetchWalletPositions(wallet, positionManager, factory) {
  const walletAddress = wallet.address;
  const walletName = wallet.name;

  // 1. Get balance
  let balance;
  try {
    balance = await positionManager.balanceOf(walletAddress);
  } catch (e) {
    console.error(`Failed to get V3 balance for ${walletName}:`, e.message);
    return [];
  }
  const count = Number(balance);

  if (count === 0) return [];

  // Fetch creation timestamps + collected fees from subgraph
  const { created: createdMap, collectedFees: collectedFeesMap } = await getV3SubgraphData(walletAddress);

  console.log(`  ${walletName} V3: ${count} NFTs found`);

  // 2. Get all token IDs (batched to avoid rate limits)
  const indices = Array.from({ length: count }, (_, i) => i);
  const tokenIds = await batchedAll(indices, (i) =>
    withRetry(() => positionManager.tokenOfOwnerByIndex(walletAddress, i))
  );

  // 3. Get all positions (batched)
  const rawPositions = await batchedAll(tokenIds, (id) =>
    withRetry(() => positionManager.positions(id))
  );

  // 4. Process ONLY active positions (skip closed ones entirely)
  const positions = [];

  for (let i = 0; i < rawPositions.length; i++) {
    const pos = rawPositions[i];
    const liquidity = pos.liquidity;

    // Skip closed positions — no need to query pool/fees
    if (Number(liquidity) === 0) continue;

    const token0Info = await getTokenInfo(pos.token0);
    const token1Info = await getTokenInfo(pos.token1);

    // Get pool address and current price (with retry)
    let poolAddress, sqrtPriceX96, currentTick;
    try {
      poolAddress = await withRetry(() => factory.getPool(pos.token0, pos.token1, pos.fee));
      if (poolAddress === ethers.ZeroAddress) throw new Error('Pool not found');
      const pool = new ethers.Contract(poolAddress, POOL_ABI, provider);
      const slot0 = await withRetry(() => pool.slot0());
      sqrtPriceX96 = slot0.sqrtPriceX96;
      currentTick = Number(slot0.tick);
    } catch (e) {
      console.error(`Failed to get pool for position ${tokenIds[i]} (${walletName}):`, e.message);
      continue;
    }
    await sleep(100);

    const tickLower = Number(pos.tickLower);
    const tickUpper = Number(pos.tickUpper);
    const inRange = currentTick >= tickLower && currentTick < tickUpper;

    const currentPrice = sqrtPriceX96ToPrice(sqrtPriceX96, token0Info.decimals, token1Info.decimals);
    const lowerPrice = tickToPrice(tickLower, token0Info.decimals, token1Info.decimals);
    const upperPrice = tickToPrice(tickUpper, token0Info.decimals, token1Info.decimals);

    const { amount0, amount1 } = getTokenAmounts(
      liquidity, sqrtPriceX96, tickLower, tickUpper,
      token0Info.decimals, token1Info.decimals
    );

    // Uncollected fees
    const { fees0, fees1 } = await getUnclaimedFees(positionManager, tokenIds[i], walletAddress);
    const feesOwed0 = Number(fees0) / Math.pow(10, token0Info.decimals);
    const feesOwed1 = Number(fees1) / Math.pow(10, token1Info.decimals);

    positions.push({
      tokenId: tokenIds[i].toString(),
      token0: token0Info,
      token1: token1Info,
      token0addr: pos.token0,
      token1addr: pos.token1,
      fee: Number(pos.fee),
      feeLabel: feeLabel(pos.fee),
      tickLower,
      tickUpper,
      currentTick,
      liquidity: liquidity.toString(),
      liquidityActive: true,
      inRange,
      currentPrice,
      lowerPrice,
      upperPrice,
      amount0,
      amount1,
      feesOwed0,
      feesOwed1,
      poolAddress,
      walletName,
      walletAddress,
      protocol: 'V3',
      createdAt: createdMap[tokenIds[i].toString()] || 0,
      lastCollectAt: 0, // filled after all positions fetched
      collectedFees: collectedFeesMap[tokenIds[i].toString()] || { token0: 0, token1: 0 },
    });
  }

  return positions;
}

// --- Main fetch (all wallets) ---
async function fetchPositions(forceRefresh = false) {
  if (!forceRefresh && cache.data && (Date.now() - cache.timestamp < CACHE_TTL)) {
    return cache.data;
  }
  // Clear all caches on force refresh
  if (forceRefresh) {
    for (const k of Object.keys(v4IdCache)) delete v4IdCache[k];
    for (const k of Object.keys(v3CollectCache)) delete v3CollectCache[k];
    for (const k of Object.keys(v3CreatedChainCache)) delete v3CreatedChainCache[k];
    console.log('Force refresh: cleared all caches');
  }

  const v3pm = new ethers.Contract(V3_POSITION_MANAGER, V3_POSITION_MANAGER_ABI, provider);
  const factory = new ethers.Contract(V3_FACTORY, FACTORY_ABI, provider);
  const v4pm = new ethers.Contract(V4_POSITION_MANAGER, V4_POSITION_MANAGER_ABI, provider);
  const stateView = new ethers.Contract(V4_STATE_VIEW, V4_STATE_VIEW_ABI, provider);

  console.log(`Fetching V3+V4 positions for ${WALLETS.length} wallets (concurrency: ${MAX_CONCURRENT})...`);

  // Fetch all wallets: V3 + V4 combined
  const walletResults = await asyncPool(MAX_CONCURRENT, WALLETS, async (wallet) => {
    const [v3positions, v4positions] = await Promise.all([
      fetchWalletPositions(wallet, v3pm, factory),
      fetchWalletV4Positions(wallet, v4pm, stateView),
    ]);
    const allPositions = [...v3positions, ...v4positions];
    await sleep(500);
    return { address: wallet.address, name: wallet.name, positions: allPositions, totalUSD: 0 };
  });

  // For active V3 positions: fill in createdAt (chain fallback) + lastCollectAt
  const activeV3Positions = [];
  for (const wr of walletResults) {
    for (const pos of wr.positions) {
      if (pos.protocol === 'V3' && pos.liquidityActive) {
        activeV3Positions.push(pos);
      }
    }
  }

  if (activeV3Positions.length > 0) {
    // 1. Fill createdAt for positions missing it
    const needCreatedAt = activeV3Positions.filter(p => !p.createdAt);
    if (needCreatedAt.length > 0) {
      console.log(`Fetching mint times for ${needCreatedAt.length} V3 positions missing createdAt...`);
      for (const pos of needCreatedAt) {
        pos.createdAt = await getV3MintTime(BigInt(pos.tokenId));
      }
    }

    // 2. Fetch Collect events
    const tokenIds = activeV3Positions.map(p => BigInt(p.tokenId));
    console.log(`Fetching Collect events for ${tokenIds.length} active V3 positions...`);
    const collectTimes = await getV3LastCollectTimes(tokenIds);
    for (const pos of activeV3Positions) {
      pos.lastCollectAt = collectTimes[pos.tokenId] || 0;
    }
    console.log(`Collect events: found ${Object.keys(collectTimes).length} positions with collects`);
  }

  // For active V4 positions: fill in lastCollectAt via PoolManager ModifyLiquidity events
  const activeV4Positions = [];
  for (const wr of walletResults) {
    for (const pos of wr.positions) {
      if (pos.protocol === 'V4' && pos.liquidityActive) {
        activeV4Positions.push(pos);
      }
    }
  }

  if (activeV4Positions.length > 0) {
    console.log(`Fetching Collect events for ${activeV4Positions.length} active V4 positions...`);
    const v4Inputs = activeV4Positions.map(p => ({
      tokenId: p.tokenId,
      poolId: p.poolAddress,
      tickLower: p.tickLower,
      tickUpper: p.tickUpper,
    }));
    const v4CollectTimes = await getV4LastCollectTimes(v4Inputs);
    for (const pos of activeV4Positions) {
      pos.lastCollectAt = v4CollectTimes[pos.tokenId] || 0;
    }
    console.log(`V4 Collect events: found ${Object.keys(v4CollectTimes).length} positions with collects`);
  }

  // Collect all token addresses and all positions for USD pricing
  const allTokenAddresses = new Set();
  const allPositions = [];

  for (const wr of walletResults) {
    for (const pos of wr.positions) {
      allTokenAddresses.add(pos.token0addr.toLowerCase());
      allTokenAddresses.add(pos.token1addr.toLowerCase());
      allPositions.push(pos);
    }
  }

  // Get USD prices (once for all tokens)
  const usdPrices = await getUSDPrices([...allTokenAddresses], allPositions);

  // Calculate USD values
  let grandTotalUSD = 0;
  let totalActive = 0;
  let totalInRange = 0;
  let totalOutOfRange = 0;
  let totalFees = 0;
  let walletsWithActiveLP = 0;

  for (const wr of walletResults) {
    let walletTotal = 0;
    let hasActive = false;

    for (const pos of wr.positions) {
      const price0 = usdPrices[pos.token0.address.toLowerCase()] || 0;
      const price1 = usdPrices[pos.token1.address.toLowerCase()] || 0;

      pos.token0USD = price0;
      pos.token1USD = price1;
      pos.positionValueUSD = pos.amount0 * price0 + pos.amount1 * price1;
      pos.feesValueUSD = pos.feesOwed0 * price0 + pos.feesOwed1 * price1;
      pos.totalValueUSD = pos.positionValueUSD + pos.feesValueUSD;

      // === Two daily rate metrics ===
      // 1. Cumulative daily rate: from creation, total fees (collected + pending) / principal / total days
      if (pos.createdAt > 0 && pos.positionValueUSD >= 10) {
        const totalMs = Date.now() - pos.createdAt;
        const totalDays = totalMs / (24 * 60 * 60 * 1000);
        const totalHours = totalMs / (60 * 60 * 1000);
        if (totalDays > 0) {
          // Total fees = already collected (from subgraph) + pending (unclaimed)
          const cf = pos.collectedFees || { token0: 0, token1: 0 };
          const collectedUSD = cf.token0 * price0 + cf.token1 * price1;
          const totalFeesUSD = collectedUSD + pos.feesValueUSD;
          if (totalFeesUSD > 0) {
            pos.dailyRateCumulative = (totalFeesUSD / pos.positionValueUSD) / totalDays * 100;
          }
          pos.totalDays = totalDays >= 1 ? Math.floor(totalDays) : 0;
          pos.totalHours = Math.floor(totalHours);
          pos.totalMinutes = Math.floor((totalMs % (60 * 60 * 1000)) / (60 * 1000));
        }
      }

      // 2. Current daily rate: from last collect (or creation if never collected), pending fees only
      const currentStart = pos.lastCollectAt || pos.createdAt;
      if (currentStart > 0 && pos.positionValueUSD >= 10 && pos.feesValueUSD > 0) {
        const holdMs = Date.now() - currentStart;
        const holdDays = holdMs / (24 * 60 * 60 * 1000);
        const holdHours = holdMs / (60 * 60 * 1000);
        if (holdDays > 0) {
          pos.dailyRateCurrent = (pos.feesValueUSD / pos.positionValueUSD) / holdDays * 100;
          pos.holdDays = holdDays >= 1 ? Math.floor(holdDays) : 0;
          pos.holdHours = Math.floor(holdHours);
          pos.holdMinutes = Math.floor((holdMs % (60 * 60 * 1000)) / (60 * 1000));
          pos.hasCollected = !!pos.lastCollectAt;
        }
      }

      walletTotal += pos.totalValueUSD;
      totalFees += pos.feesValueUSD;

      if (pos.liquidityActive) {
        totalActive++;
        hasActive = true;
        if (pos.inRange) totalInRange++;
        else totalOutOfRange++;
      }
    }

    wr.totalUSD = walletTotal;
    grandTotalUSD += walletTotal;
    if (hasActive) walletsWithActiveLP++;

    // Normalize price direction (Token/USDT) and sort
    wr.positions = wr.positions.map(normalizePosition);
    wr.positions.sort((a, b) => {
      if (a.liquidityActive && !b.liquidityActive) return -1;
      if (!a.liquidityActive && b.liquidityActive) return 1;
      return b.totalValueUSD - a.totalValueUSD;
    });
  }

  // Only include wallets that have positions (balanceOf > 0)
  const wallets = walletResults.filter(wr => wr.positions.length > 0);

  // Sort wallets by name (马年1号, 马年2号, ...)
  wallets.sort((a, b) => {
    const numA = parseInt((a.name.match(/\d+/) || ['0'])[0]);
    const numB = parseInt((b.name.match(/\d+/) || ['0'])[0]);
    return numA - numB;
  });

  const result = {
    wallets,
    grandTotalUSD,
    timestamp: Date.now(),
    stats: {
      totalActive,
      totalInRange,
      totalOutOfRange,
      totalFees,
      walletsWithActiveLP,
      totalWallets: WALLETS.length,
    },
  };

  cache = { data: result, timestamp: Date.now() };
  console.log(`Fetch complete. ${wallets.length} wallets with positions, ${totalActive} active positions, grand total: $${grandTotalUSD.toFixed(2)}`);
  return result;
}

// --- Normalize price direction: always Token/USDT ---
function normalizePosition(pos) {
  const t0addr = (pos.token0addr || pos.token0.address || '').toLowerCase();
  // If token0 is a stablecoin, swap sides so display is Token/USDT
  if (STABLECOINS.has(t0addr)) {
    return {
      ...pos,
      token0: pos.token1,
      token1: pos.token0,
      token0addr: pos.token1addr,
      token1addr: pos.token0addr,
      token0USD: pos.token1USD,
      token1USD: pos.token0USD,
      amount0: pos.amount1,
      amount1: pos.amount0,
      feesOwed0: pos.feesOwed1,
      feesOwed1: pos.feesOwed0,
      currentPrice: pos.currentPrice > 0 ? 1 / pos.currentPrice : 0,
      lowerPrice: pos.upperPrice > 0 ? 1 / pos.upperPrice : 0,  // swap & invert
      upperPrice: pos.lowerPrice > 0 ? 1 / pos.lowerPrice : 0,  // swap & invert
      _normalized: true,
    };
  }
  return pos;
}

// --- Routes ---
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/health', (req, res) => {
  res.json({
    keys: GRAPH_KEYS.map((k, i) => ({
      index: i,
      ok: k.ok,
      fail: k.fail,
      blocked: k.blockedUntil > Date.now(),
      blockedUntilISO: k.blockedUntil > Date.now() ? new Date(k.blockedUntil).toISOString() : null,
    })),
    cache: {
      mainCacheFresh: cache.data ? (Date.now() - cache.timestamp < CACHE_TTL) : false,
      mainCacheAge: cache.timestamp ? Math.round((Date.now() - cache.timestamp) / 1000) + 's' : null,
      v4IdCacheEntries: Object.keys(v4IdCache).length,
    },
  });
});

// --- Wallet management API ---
app.get('/api/wallets', (req, res) => {
  res.json(WALLETS);
});

app.post('/api/wallets', (req, res) => {
  const { address, name } = req.body;
  if (!address || !name) return res.status(400).json({ error: '需要 address 和 name' });
  const addr = address.trim().toLowerCase();
  if (!/^0x[0-9a-f]{40}$/.test(addr)) return res.status(400).json({ error: '无效的 BSC 地址' });
  if (WALLETS.some(w => w.address.toLowerCase() === addr)) return res.status(409).json({ error: '地址已存在' });
  if (WALLETS.length >= 30) return res.status(400).json({ error: '最多支持 30 个地址' });
  WALLETS.push({ address: addr, name: name.trim() });
  saveWallets(WALLETS);
  cache = { data: null, timestamp: 0 }; // clear cache so next fetch uses new list
  console.log(`Wallet added: ${name.trim()} (${addr})`);
  res.json({ ok: true, wallets: WALLETS });
});

app.delete('/api/wallets/:address', (req, res) => {
  const addr = req.params.address.toLowerCase();
  const idx = WALLETS.findIndex(w => w.address.toLowerCase() === addr);
  if (idx === -1) return res.status(404).json({ error: '地址不存在' });
  const removed = WALLETS.splice(idx, 1)[0];
  saveWallets(WALLETS);
  cache = { data: null, timestamp: 0 };
  console.log(`Wallet removed: ${removed.name} (${addr})`);
  res.json({ ok: true, wallets: WALLETS });
});

app.get('/api/positions', async (req, res) => {
  try {
    const forceRefresh = req.query.refresh === 'true';
    const data = await fetchPositions(forceRefresh);
    res.json(data);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`LP Dashboard running at http://0.0.0.0:${PORT}`);
});
