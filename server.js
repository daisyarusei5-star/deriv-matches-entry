"use strict";

/*
|--------------------------------------------------------------------------
| DERIV LIVE ENTRY
| POWERED BY ELISY 254
|--------------------------------------------------------------------------
| Analysis-only/manual entry application.
| NO automatic trading.
| NO buy/sell calls.
|
| Main responsibilities:
|   1. Login/session handling
|   2. Dynamic Deriv market discovery
|   3. Market discovery caching / 429 protection
|   4. Browser WebSocket endpoint: /live
|   5. Exact-symbol tick streaming
|   6. Tick history delivery
|   7. MATCHES activation
|--------------------------------------------------------------------------
*/

const express = require("express");
const session = require("express-session");
const http = require("http");
const path = require("path");
const WebSocket = require("ws");

/*
|--------------------------------------------------------------------------
| APP CONFIGURATION
|--------------------------------------------------------------------------
*/

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 3000);

const DERIV_APP_ID = String(process.env.DERIV_APP_ID || "").trim();

const SESSION_SECRET =
  process.env.SESSION_SECRET ||
  "ELISY254_CHANGE_THIS_TO_A_LONG_RANDOM_SECRET";

const LOGIN_MARKET =
  process.env.LOGIN_MARKET ||
  "Market23";

const LOGIN_PASSWORD =
  process.env.LOGIN_PASSWORD ||
  "Trade23";

const MATCHES_CODE =
  process.env.MATCHES_CODE ||
  "19809";

/*
|--------------------------------------------------------------------------
| DERIV ENDPOINTS
|--------------------------------------------------------------------------
|
| Current public Deriv endpoint:
|   wss://api.derivws.com/trading/v1/options/ws/public
|
| Legacy public endpoint:
|   wss://ws.binaryws.com/websockets/v3
|
| The current endpoint is preferred.
| The legacy endpoint is used as a fallback if necessary.
|--------------------------------------------------------------------------
*/

const DERIV_PUBLIC_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const DERIV_LEGACY_WS =
  "wss://ws.binaryws.com/websockets/v3";

/*
|--------------------------------------------------------------------------
| MARKET DISCOVERY SETTINGS
|--------------------------------------------------------------------------
|
| Keeping a cache is important.
|
| Without caching:
|
| Browser -> /api/markets
|          -> new Deriv WebSocket
|          -> active_symbols
|
| Every page refresh can create another connection.
|
| That can result in HTTP 429 rate limiting.
|--------------------------------------------------------------------------
*/

const MARKET_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

const MARKET_DISCOVERY_TIMEOUT_MS = 15000;

const MARKET_DISCOVERY_RETRY_DELAY_MS = 2500;

let marketCache = null;

let marketCacheUpdatedAt = 0;

let marketDiscoveryPromise = null;

let lastMarketDiscoveryError = null;

/*
|--------------------------------------------------------------------------
| EXPRESS CONFIGURATION
|--------------------------------------------------------------------------
*/

app.set("trust proxy", 1);

app.use(express.json());

app.use(express.urlencoded({ extended: true }));

/*
|--------------------------------------------------------------------------
| SESSION
|--------------------------------------------------------------------------
*/

const sessionParser = session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    maxAge: 24 * 60 * 60 * 1000
  }
});

app.use(sessionParser);

/*
|--------------------------------------------------------------------------
| STATIC FILES
|--------------------------------------------------------------------------
*/

app.use(express.static(path.join(__dirname)));

/*
|--------------------------------------------------------------------------
| BASIC HEALTH CHECK
|--------------------------------------------------------------------------
*/

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "DERIV LIVE ENTRY",
    timestamp: new Date().toISOString()
  });
});

/*
|--------------------------------------------------------------------------
| HELPER FUNCTIONS
|--------------------------------------------------------------------------
*/

function normalizeText(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function normalizeSymbol(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function isAuthenticated(req) {
  return Boolean(
    req.session &&
    req.session.authenticated === true
  );
}

function sendJsonError(res, status, message, extra = {}) {
  return res.status(status).json({
    ok: false,
    error: message,
    ...extra
  });
}

/*
|--------------------------------------------------------------------------
| PRECISION HELPERS
|--------------------------------------------------------------------------
*/

function precisionFromPip(pip) {
  const numericPip = Number(pip);

  if (!Number.isFinite(numericPip) || numericPip <= 0) {
    return 2;
  }

  let precision = 0;
  let value = numericPip;

  while (
    precision < 10 &&
    Math.abs(value - Math.round(value)) > 1e-12
  ) {
    value *= 10;
    precision++;
  }

  return precision;
}

function getLastDigitFromQuote(quote, precision = 2) {
  const numericQuote = Number(quote);

  if (!Number.isFinite(numericQuote)) {
    return null;
  }

  /*
  |--------------------------------------------------------------------------
  | IMPORTANT
  |--------------------------------------------------------------------------
  | Convert using the market precision before extracting the final digit.
  |--------------------------------------------------------------------------
  */

  const fixed = numericQuote.toFixed(
    Number.isInteger(precision) ? precision : 2
  );

  const digits = fixed.replace(/\D/g, "");

  if (!digits.length) {
    return null;
  }

  return Number(digits.charAt(digits.length - 1));
}

/*
|--------------------------------------------------------------------------
| MARKET NAME FILTER
|--------------------------------------------------------------------------
|
| We dynamically discover markets from Deriv.
|
| We only expose:
|
|   - Volatility markets
|   - Jump markets
|
| We do NOT hard-code individual symbols.
|--------------------------------------------------------------------------
*/

function isWantedMarket(item) {
  if (!item || typeof item !== "object") {
    return false;
  }

  const symbol = String(
    item.underlying_symbol ||
    item.symbol ||
    ""
  ).trim();

  const displayName = String(
    item.underlying_symbol_name ||
    item.display_name ||
    item.name ||
    ""
  ).trim();

  const market = String(
    item.market ||
    item.market_name ||
    ""
  ).trim();

  const subgroup = String(
    item.subgroup ||
    item.submarket ||
    ""
  ).trim();

  const combined = normalizeText(
    [
      symbol,
      displayName,
      market,
      subgroup
    ].join(" ")
  );

  return (
    combined.includes("volatility") ||
    combined.includes("jump")
  );
}

/*
|--------------------------------------------------------------------------
| MARKET OPEN / TRADING FILTER
|--------------------------------------------------------------------------
*/

function isTradableMarket(item) {
  if (!item || typeof item !== "object") {
    return false;
  }

  /*
  |--------------------------------------------------------------------------
  | Some Deriv responses contain these fields.
  | If they are absent, we do not reject the market.
  |--------------------------------------------------------------------------
  */

  if (
    Object.prototype.hasOwnProperty.call(item, "is_trading_suspended") &&
    Number(item.is_trading_suspended) === 1
  ) {
    return false;
  }

  if (
    Object.prototype.hasOwnProperty.call(item, "exchange_is_open") &&
    Number(item.exchange_is_open) === 0
  ) {
    return false;
  }

  return true;
}

/*
|--------------------------------------------------------------------------
| NORMALIZE DERIV MARKET
|--------------------------------------------------------------------------
|
| Supports both current and legacy active_symbols response formats.
|--------------------------------------------------------------------------
*/

function normalizeMarket(raw) {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const symbol = String(
    raw.underlying_symbol ||
    raw.symbol ||
    ""
  ).trim();

  const name = String(
    raw.underlying_symbol_name ||
    raw.display_name ||
    raw.name ||
    ""
  ).trim();

  if (!symbol || !name) {
    return null;
  }

  const pip =
    raw.pip_size ??
    raw.pip ??
    raw.pip_size_value ??
    null;

  const precision = precisionFromPip(pip);

  return {
    symbol,
    name,

    /*
     * Keep these fields for compatibility with the frontend.
     */
    display_name: name,
    underlying_symbol: symbol,
    underlying_symbol_name: name,

    market: String(
      raw.market ||
      raw.market_name ||
      ""
    ).trim(),

    subgroup: String(
      raw.subgroup ||
      raw.submarket ||
      ""
    ).trim(),

    pip_size:
      Number.isFinite(Number(pip))
        ? Number(pip)
        : null,

    precision,

    exchange_is_open:
      raw.exchange_is_open !== undefined
        ? Number(raw.exchange_is_open)
        : null,

    is_trading_suspended:
      raw.is_trading_suspended !== undefined
        ? Number(raw.is_trading_suspended)
        : null
  };
}

/*
|--------------------------------------------------------------------------
| SORT MARKETS
|--------------------------------------------------------------------------
*/

function sortMarkets(markets) {
  return markets.sort((a, b) => {
    const nameA = normalizeText(a.name);
    const nameB = normalizeText(b.name);

    return nameA.localeCompare(
      nameB,
      undefined,
      {
        numeric: true,
        sensitivity: "base"
      }
    );
  });
}

/*
|--------------------------------------------------------------------------
| BUILD MARKET LIST
|--------------------------------------------------------------------------
*/

function buildMarketList(rawMarkets) {
  if (!Array.isArray(rawMarkets)) {
    return [];
  }

  const seen = new Set();

  const markets = [];

  for (const raw of rawMarkets) {
    if (!raw || typeof raw !== "object") {
      continue;
    }

    if (!isWantedMarket(raw)) {
      continue;
    }

    if (!isTradableMarket(raw)) {
      continue;
    }

    const market = normalizeMarket(raw);

    if (!market) {
      continue;
    }

    const key = normalizeSymbol(market.symbol);

    if (!key || seen.has(key)) {
      continue;
    }

    seen.add(key);

    markets.push(market);
  }

  return sortMarkets(markets);
}

/*
|--------------------------------------------------------------------------
| DERIV WEBSOCKET CONNECTOR
|--------------------------------------------------------------------------
*/

function connectDerivWebSocket(url, timeoutMs = MARKET_DISCOVERY_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    let settled = false;

    let ws;

    let timeoutHandle;

    function cleanup() {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
        timeoutHandle = null;
      }
    }

    function fail(error) {
      if (settled) {
        return;
      }

      settled = true;

      cleanup();

      try {
        if (ws) {
          ws.removeAllListeners();
          ws.close();
        }
      } catch (_) {
        // Ignore cleanup errors.
      }

      reject(error);
    }

    function succeed(value) {
      if (settled) {
        return;
      }

      settled = true;

      cleanup();

      resolve(value);
    }

    try {
      ws = new WebSocket(url);
    } catch (error) {
      fail(error);
      return;
    }

    timeoutHandle = setTimeout(() => {
      const error = new Error(
        `Deriv WebSocket connection timeout after ${timeoutMs}ms`
      );

      error.code = "DERIV_TIMEOUT";

      fail(error);
    }, timeoutMs);

    ws.once("open", () => {
      succeed(ws);
    });

    ws.once("error", (error) => {
      fail(error);
    });

    ws.once("unexpected-response", (request, response) => {
      const statusCode = response && response.statusCode
        ? response.statusCode
        : "unknown";

      const error = new Error(
        `Unexpected server response: ${statusCode}`
      );

      error.code =
        statusCode === 429
          ? "DERIV_RATE_LIMITED"
          : "DERIV_UNEXPECTED_RESPONSE";

      error.statusCode = statusCode;

      fail(error);
    });
  });
}

/*
|--------------------------------------------------------------------------
| REQUEST ACTIVE SYMBOLS
|--------------------------------------------------------------------------
*/

function requestActiveSymbols(ws, useLegacyFormat = false) {
  return new Promise((resolve, reject) => {
    let settled = false;

    let timeoutHandle;

    function cleanup() {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
        timeoutHandle = null;
      }

      ws.removeListener("message", onMessage);

      ws.removeListener("error", onError);

      ws.removeListener("close", onClose);
    }

    function finishSuccess(value) {
      if (settled) {
        return;
      }

      settled = true;

      cleanup();

      resolve(value);
    }

    function finishError(error) {
      if (settled) {
        return;
      }

      settled = true;

      cleanup();

      reject(error);
    }

    function onError(error) {
      finishError(error);
    }

    function onClose() {
      finishError(
        new Error(
          "Deriv WebSocket closed before active_symbols response."
        )
      );
    }

    function onMessage(rawMessage) {
      let data;

      try {
        const text = Buffer.isBuffer(rawMessage)
          ? rawMessage.toString("utf8")
          : String(rawMessage);

        data = JSON.parse(text);
      } catch (_) {
        return;
      }

      /*
      |--------------------------------------------------------------------------
      | Error returned by Deriv
      |--------------------------------------------------------------------------
      */

      if (data.error) {
        const error = new Error(
          data.error.message ||
          data.error.code ||
          "Deriv active_symbols request failed."
        );

        error.derivCode = data.error.code;

        finishError(error);

        return;
      }

      /*
      |--------------------------------------------------------------------------
      | active_symbols response
      |--------------------------------------------------------------------------
      */

      if (
        data.msg_type === "active_symbols" ||
        Array.isArray(data.active_symbols)
      ) {
        finishSuccess(
          Array.isArray(data.active_symbols)
            ? data.active_symbols
            : []
        );
      }
    }

    ws.on("message", onMessage);

    ws.on("error", onError);

    ws.on("close", onClose);

    timeoutHandle = setTimeout(() => {
      finishError(
        new Error(
          "Timed out waiting for Deriv active_symbols response."
        )
      );
    }, MARKET_DISCOVERY_TIMEOUT_MS);

    /*
    |--------------------------------------------------------------------------
    | Current API
    |--------------------------------------------------------------------------
    |
    | The current active_symbols request does not require product_type.
    |--------------------------------------------------------------------------
    */

    const request = useLegacyFormat
      ? {
          active_symbols: "brief",
          product_type: "basic",
          req_id: 1001
        }
      : {
          active_symbols: "brief",
          req_id: 1001
        };

    try {
      ws.send(JSON.stringify(request));
    } catch (error) {
      finishError(error);
    }
  });
}

/*
|--------------------------------------------------------------------------
| CLOSE DERIV SOCKET SAFELY
|--------------------------------------------------------------------------
*/

function closeDerivSocket(ws) {
  if (!ws) {
    return;
  }

  try {
    if (
      ws.readyState === WebSocket.OPEN ||
      ws.readyState === WebSocket.CONNECTING
    ) {
      ws.close();
    }
  } catch (_) {
    // Ignore close errors.
  }
}

/*
|--------------------------------------------------------------------------
| DISCOVER USING CURRENT DERIV ENDPOINT
|--------------------------------------------------------------------------
*/

async function discoverMarketsCurrent() {
  console.log(
    "Market discovery: connecting to current Deriv public WebSocket..."
  );

  const ws = await connectDerivWebSocket(
    DERIV_PUBLIC_WS
  );

  try {
    const rawMarkets = await requestActiveSymbols(
      ws,
      false
    );

    const markets = buildMarketList(rawMarkets);

    if (!markets.length) {
      throw new Error(
        "Deriv current API returned no active Volatility or Jump markets."
      );
    }

    console.log(
      `Market discovery: current API returned ${markets.length} matching markets.`
    );

    return {
      markets,
      source: "current"
    };
  } finally {
    closeDerivSocket(ws);
  }
}

/*
|--------------------------------------------------------------------------
| DISCOVER USING LEGACY DERIV ENDPOINT
|--------------------------------------------------------------------------
*/

async function discoverMarketsLegacy() {
  let url = DERIV_LEGACY_WS;

  /*
  |--------------------------------------------------------------------------
  | If DERIV_APP_ID exists, use it only for the legacy endpoint.
  |
  | The current public endpoint does not need an app ID.
  |--------------------------------------------------------------------------
  */

  if (DERIV_APP_ID) {
    url =
      `${DERIV_LEGACY_WS}?app_id=` +
      encodeURIComponent(DERIV_APP_ID);
  }

  console.log(
    "Market discovery: trying legacy Deriv public WebSocket fallback..."
  );

  const ws = await connectDerivWebSocket(url);

  try {
    const rawMarkets = await requestActiveSymbols(
      ws,
      true
    );

    const markets = buildMarketList(rawMarkets);

    if (!markets.length) {
      throw new Error(
        "Deriv legacy API returned no active Volatility or Jump markets."
      );
    }

    console.log(
      `Market discovery: legacy API returned ${markets.length} matching markets.`
    );

    return {
      markets,
      source: "legacy"
    };
  } finally {
    closeDerivSocket(ws);
  }
}

/*
|--------------------------------------------------------------------------
| DISCOVER MARKETS WITH RETRY / FALLBACK
|--------------------------------------------------------------------------
*/

async function discoverMarketsFromDeriv() {
  let currentError = null;

  /*
  |--------------------------------------------------------------------------
  | First attempt: current Deriv API.
  |--------------------------------------------------------------------------
  */

  try {
    return await discoverMarketsCurrent();
  } catch (error) {
    currentError = error;

    console.error(
      "Market discovery current endpoint failed:",
      error.message
    );

    if (error.statusCode === 429) {
      console.error(
        "Deriv returned HTTP 429. Waiting briefly before fallback..."
      );

      await new Promise((resolve) => {
        setTimeout(
          resolve,
          MARKET_DISCOVERY_RETRY_DELAY_MS
        );
      });
    }
  }

  /*
  |--------------------------------------------------------------------------
  | Second attempt: legacy public API.
  |--------------------------------------------------------------------------
  */

  try {
    return await discoverMarketsLegacy();
  } catch (legacyError) {
    console.error(
      "Market discovery legacy endpoint failed:",
      legacyError.message
    );

    const finalError = new Error(
      [
        "Unable to discover Deriv markets.",
        `Current API: ${currentError?.message || "failed"}`,
        `Legacy API: ${legacyError?.message || "failed"}`
      ].join(" ")
    );

    finalError.currentError = currentError;

    finalError.legacyError = legacyError;

    throw finalError;
  }
}

/*
|--------------------------------------------------------------------------
| GET MARKET CACHE
|--------------------------------------------------------------------------
*/

function getCachedMarkets() {
  if (!marketCache) {
    return null;
  }

  const age = Date.now() - marketCacheUpdatedAt;

  if (age > MARKET_CACHE_TTL_MS) {
    return null;
  }

  return marketCache;
}

/*
|--------------------------------------------------------------------------
| DISCOVER MARKETS WITH CACHE
|--------------------------------------------------------------------------
|
| This is the main 429 protection mechanism.
|
| If 10 browsers request /api/markets at once:
|
|   Browser 1 -> starts discovery
|   Browser 2 -> waits for same promise
|   Browser 3 -> waits for same promise
|   ...
|
| We do NOT create 10 Deriv WebSocket connections.
|--------------------------------------------------------------------------
*/

async function getMarkets() {
  const cached = getCachedMarkets();

  if (cached) {
    return {
      ...cached,
      cached: true,
      cache_age_ms:
        Date.now() - marketCacheUpdatedAt
    };
  }

  /*
  |--------------------------------------------------------------------------
  | If another discovery is already running, wait for it.
  |--------------------------------------------------------------------------
  */

  if (marketDiscoveryPromise) {
    return await marketDiscoveryPromise;
  }

  marketDiscoveryPromise = (async () => {
    try {
      const result =
        await discoverMarketsFromDeriv();

      marketCache = {
        markets: result.markets,
        source: result.source
      };

      marketCacheUpdatedAt = Date.now();

      lastMarketDiscoveryError = null;

      return {
        ...marketCache,
        cached: false,
        cache_age_ms: 0
      };
    } catch (error) {
      lastMarketDiscoveryError = {
        message: error.message,
        timestamp: new Date().toISOString()
      };

      throw error;
    } finally {
      marketDiscoveryPromise = null;
    }
  })();

  return await marketDiscoveryPromise;
}

/*
|--------------------------------------------------------------------------
| LOGIN
|--------------------------------------------------------------------------
*/

app.post("/api/login", (req, res) => {
  const market = String(
    req.body?.market || ""
  ).trim();

  const password = String(
    req.body?.password || ""
  ).trim();

  if (!market || !password) {
    return sendJsonError(
      res,
      400,
      "Market and password are required."
    );
  }

  if (
    market !== LOGIN_MARKET ||
    password !== LOGIN_PASSWORD
  ) {
    return sendJsonError(
      res,
      401,
      "Invalid market or password."
    );
  }

  req.session.authenticated = true;

  req.session.loginMarket = market;

  req.session.save((error) => {
    if (error) {
      console.error(
        "Session save error:",
        error
      );

      return sendJsonError(
        res,
        500,
        "Unable to create login session."
      );
    }

    return res.json({
      ok: true,
      message: "Login successful."
    });
  });
});

/*
|--------------------------------------------------------------------------
| SESSION CHECK
|--------------------------------------------------------------------------
*/

app.get("/api/session", (req, res) => {
  return res.json({
    ok: true,
    authenticated: isAuthenticated(req)
  });
});

/*
|--------------------------------------------------------------------------
| LOGOUT
|--------------------------------------------------------------------------
*/

app.post("/api/logout", (req, res) => {
  req.session.destroy((error) => {
    if (error) {
      console.error(
        "Logout/session destroy error:",
        error
      );

      return sendJsonError(
        res,
        500,
        "Unable to log out."
      );
    }

    res.clearCookie("connect.sid");

    return res.json({
      ok: true,
      message: "Logged out."
    });
  });
});

/*
|--------------------------------------------------------------------------
| MATCHES ACTIVATION
|--------------------------------------------------------------------------
*/

app.post("/api/unlock-matches", (req, res) => {
  if (!isAuthenticated(req)) {
    return sendJsonError(
      res,
      401,
      "You must be logged in."
    );
  }

  const code = String(
    req.body?.code || ""
  ).trim();

  if (!code) {
    return sendJsonError(
      res,
      400,
      "Activation code is required."
    );
  }

  if (code !== MATCHES_CODE) {
    return sendJsonError(
      res,
      403,
      "Invalid MATCHES activation code."
    );
  }

  req.session.matchesUnlocked = true;

  req.session.save((error) => {
    if (error) {
      console.error(
        "MATCHES session save error:",
        error
      );

      return sendJsonError(
        res,
        500,
        "Unable to activate MATCHES."
      );
    }

    return res.json({
      ok: true,
      unlocked: true,
      message: "MATCHES activated."
    });
  });
});

/*
|--------------------------------------------------------------------------
| MARKET DISCOVERY API
|--------------------------------------------------------------------------
*/

app.get("/api/markets", async (req, res) => {
  if (!isAuthenticated(req)) {
    return sendJsonError(
      res,
      401,
      "You must be logged in."
    );
  }

  try {
    const result = await getMarkets();

    return res.json({
      ok: true,
      markets: result.markets,
      count: result.markets.length,
      source: result.source,
      cached: result.cached,
      cache_age_ms: result.cache_age_ms,
      updated_at:
        new Date(marketCacheUpdatedAt).toISOString()
    });
  } catch (error) {
    console.error(
      "Market discovery error:",
      error
    );

    return sendJsonError(
      res,
      503,
      error.message ||
        "Unable to discover Deriv markets.",
      {
        markets: [],
        cached: false
      }
    );
  }
});

/*
|--------------------------------------------------------------------------
| OPTIONAL MARKET CACHE STATUS
|--------------------------------------------------------------------------
|
| Useful for debugging Render.
|--------------------------------------------------------------------------
*/

app.get("/api/markets/status", (req, res) => {
  if (!isAuthenticated(req)) {
    return sendJsonError(
      res,
      401,
      "You must be logged in."
    );
  }

  const cacheAge = marketCache
    ? Date.now() - marketCacheUpdatedAt
    : null;

  return res.json({
    ok: true,

    cached:
      Boolean(marketCache) &&
      cacheAge <= MARKET_CACHE_TTL_MS,

    market_count:
      marketCache?.markets?.length || 0,

    source:
      marketCache?.source || null,

    cache_age_ms:
      cacheAge,

    cache_ttl_ms:
      MARKET_CACHE_TTL_MS,

    discovery_in_progress:
      Boolean(marketDiscoveryPromise),

    last_error:
      lastMarketDiscoveryError
  });
});

/*
|--------------------------------------------------------------------------
| ROOT ROUTE
|--------------------------------------------------------------------------
*/

app.get("/", (req, res) => {
  res.sendFile(
    path.join(__dirname, "index.html")
  );
});

/*
|--------------------------------------------------------------------------
| 404 API HANDLER
|--------------------------------------------------------------------------
*/

app.use("/api", (req, res) => {
  return sendJsonError(
    res,
    404,
    "API endpoint not found."
  );
});

/*
|--------------------------------------------------------------------------
| WEBSOCKET SERVER
|--------------------------------------------------------------------------
*/

const wsServer = new WebSocket.Server({
  noServer: true
});

/*
|--------------------------------------------------------------------------
| SERVER-SIDE DERIV CONNECTION
|--------------------------------------------------------------------------
*/

function createDerivStreamingUrl() {
  /*
  |--------------------------------------------------------------------------
  | Current public endpoint.
  |
  | No app ID is required here.
  |--------------------------------------------------------------------------
  */

  return DERIV_PUBLIC_WS;
}

/*
|--------------------------------------------------------------------------
| EXACT MARKET LOOKUP
|--------------------------------------------------------------------------
*/

async function findExactMarket(
  selectedSymbol,
  selectedMarketName
) {
  const symbol =
    normalizeSymbol(selectedSymbol);

  const name =
    normalizeText(selectedMarketName);

  if (!symbol) {
    return null;
  }

  let result;

  try {
    result = await getMarkets();
  } catch (error) {
    console.error(
      "Unable to refresh market list:",
      error.message
    );

    return null;
  }

  const markets =
    Array.isArray(result.markets)
      ? result.markets
      : [];

  /*
  |--------------------------------------------------------------------------
  | Symbol is the primary identity.
  |--------------------------------------------------------------------------
  */

  const symbolMatches =
    markets.filter((market) => {
      return (
        normalizeSymbol(market.symbol) ===
        symbol
      );
    });

  if (!symbolMatches.length) {
    return null;
  }

  /*
  |--------------------------------------------------------------------------
  | If the browser supplied a market name,
  | verify it when possible.
  |--------------------------------------------------------------------------
  */

  if (name) {
    const exactName =
      symbolMatches.find((market) => {
        return (
          normalizeText(market.name) ===
          name
        );
      });

    if (exactName) {
      return exactName;
    }

    /*
    |--------------------------------------------------------------------------
    | Some Deriv responses can vary slightly in display naming.
    | Symbol remains the source of truth.
    |--------------------------------------------------------------------------
    */

    return symbolMatches[0];
  }

  return symbolMatches[0];
}

/*
|--------------------------------------------------------------------------
| SEND JSON OVER WEBSOCKET
|--------------------------------------------------------------------------
*/

function sendSocket(ws, payload) {
  if (
    !ws ||
    ws.readyState !== WebSocket.OPEN
  ) {
    return false;
  }

  try {
    ws.send(
      JSON.stringify(payload)
    );

    return true;
  } catch (error) {
    console.error(
      "Browser WebSocket send error:",
      error
    );

    return false;
  }
}

/*
|--------------------------------------------------------------------------
| START DERIV STREAM
|--------------------------------------------------------------------------
*/

async function startDerivStream(
  browserWs,
  options
) {
  const {
    selectedSymbol,
    selectedMarketName
  } = options;

  const market =
    await findExactMarket(
      selectedSymbol,
      selectedMarketName
    );

  if (!market) {
    sendSocket(
      browserWs,
      {
        type: "market_not_found",
        symbol: selectedSymbol,
        marketName: selectedMarketName,
        message:
          "Selected market is not currently available on Deriv."
      }
    );

    return;
  }

  /*
  |--------------------------------------------------------------------------
  | Do not allow the browser to select a fake/discovery market.
  |--------------------------------------------------------------------------
  */

  if (
    selectedSymbol === "__DISCOVERY_ONLY__"
  ) {
    sendSocket(
      browserWs,
      {
        type: "market_not_found",
        symbol: selectedSymbol,
        marketName: selectedMarketName,
        message:
          "Discovery-only market requests are not supported."
      }
    );

    return;
  }

  /*
  |--------------------------------------------------------------------------
  | MATCHES must be unlocked before use.
  |--------------------------------------------------------------------------
  */

  const contractType =
    String(
      options.contractType || ""
    ).trim().toUpperCase();

  if (
    contractType === "MATCHES" &&
    options.matchesUnlocked !== true
  ) {
    sendSocket(
      browserWs,
      {
        type: "deriv_error",
        message:
          "MATCHES requires activation before analysis."
      }
    );

    return;
  }

  /*
  |--------------------------------------------------------------------------
  | Close any existing Deriv connection for this browser.
  |--------------------------------------------------------------------------
  */

  if (
    browserWs.derivWs &&
    browserWs.derivWs.readyState === WebSocket.OPEN
  ) {
    try {
      browserWs.derivWs.close();
    } catch (_) {
      // Ignore.
    }
  }

  const derivUrl =
    createDerivStreamingUrl();

  let derivWs;

  try {
    derivWs =
      await connectDerivWebSocket(
        derivUrl,
        15000
      );
  } catch (error) {
    console.error(
      "Deriv streaming connection failed:",
      error
    );

    sendSocket(
      browserWs,
      {
        type: "deriv_error",
        message:
          `Unable to connect to Deriv market data: ${error.message}`
      }
    );

    return;
  }

  browserWs.derivWs = derivWs;

  browserWs.activeSymbol =
    normalizeSymbol(market.symbol);

  browserWs.activeMarketName =
    market.name;

  browserWs.activePrecision =
    Number.isInteger(market.precision)
      ? market.precision
      : 2;

  browserWs.activeContractType =
    contractType;

  browserWs.derivRequestId = 2000;

  /*
  |--------------------------------------------------------------------------
  | Notify browser.
  |--------------------------------------------------------------------------
  */

  sendSocket(
    browserWs,
    {
      type: "market_confirmed",
      symbol: market.symbol,
      marketName: market.name,
      displayName: market.name,
      precision: browserWs.activePrecision,
      pip_size: market.pip_size,
      contractType
    }
  );

  /*
  |--------------------------------------------------------------------------
  | Deriv messages
  |--------------------------------------------------------------------------
  */

  derivWs.on("message", (rawMessage) => {
    let data;

    try {
      const text =
        Buffer.isBuffer(rawMessage)
          ? rawMessage.toString("utf8")
          : String(rawMessage);

      data = JSON.parse(text);
    } catch (error) {
      console.error(
        "Invalid Deriv message:",
        error.message
      );

      return;
    }

    /*
    |--------------------------------------------------------------------------
    | Deriv API error
    |--------------------------------------------------------------------------
    */

    if (data.error) {
      console.error(
        "Deriv API error:",
        data.error
      );

      sendSocket(
        browserWs,
        {
          type: "deriv_error",
          code:
            data.error.code || null,
          message:
            data.error.message ||
            "Deriv API error."
        }
      );

      return;
    }

    /*
    |--------------------------------------------------------------------------
    | Tick history
    |--------------------------------------------------------------------------
    */

    if (
      data.msg_type === "history" &&
      data.history
    ) {
      const prices =
        Array.isArray(data.history.prices)
          ? data.history.prices
          : [];

      const times =
        Array.isArray(data.history.times)
          ? data.history.times
          : [];

      const history = [];

      for (
        let i = 0;
        i < prices.length;
        i++
      ) {
        const quote =
          Number(prices[i]);

        if (!Number.isFinite(quote)) {
          continue;
        }

        const epoch =
          Number(times[i]);

        const digit =
          getLastDigitFromQuote(
            quote,
            browserWs.activePrecision
          );

        history.push({
          quote,
          epoch:
            Number.isFinite(epoch)
              ? epoch
              : null,
          digit
        });
      }

      sendSocket(
        browserWs,
        {
          type: "history",
          symbol:
            browserWs.activeSymbol,
          marketName:
            browserWs.activeMarketName,
          precision:
            browserWs.activePrecision,
          history
        }
      );

      return;
    }

    /*
    |--------------------------------------------------------------------------
    | Live tick
    |--------------------------------------------------------------------------
    */

    if (
      data.msg_type === "tick" &&
      data.tick
    ) {
      const tick =
        data.tick;

      const tickSymbol =
        normalizeSymbol(
          tick.symbol ||
          tick.underlying_symbol ||
          ""
        );

      /*
      |--------------------------------------------------------------------------
      | HARD MARKET ISOLATION
      |--------------------------------------------------------------------------
      |
      | Never forward a tick for another market.
      |--------------------------------------------------------------------------
      */

      if (
        !tickSymbol ||
        tickSymbol !==
          browserWs.activeSymbol
      ) {
        return;
      }

      const quote =
        Number(tick.quote);

      if (!Number.isFinite(quote)) {
        return;
      }

      const epoch =
        Number(tick.epoch);

      const digit =
        getLastDigitFromQuote(
          quote,
          browserWs.activePrecision
        );

      sendSocket(
        browserWs,
        {
          type: "tick",
          symbol: tickSymbol,
          marketName:
            browserWs.activeMarketName,
          quote,
          epoch:
            Number.isFinite(epoch)
              ? epoch
              : null,
          digit,
          precision:
            browserWs.activePrecision
        }
      );

      return;
    }
  });

  /*
  |--------------------------------------------------------------------------
  | Deriv connection errors
  |--------------------------------------------------------------------------
  */

  derivWs.on("error", (error) => {
    console.error(
      "Deriv streaming error:",
      error.message
    );

    sendSocket(
      browserWs,
      {
        type: "deriv_error",
        message:
          error.message ||
          "Deriv streaming error."
      }
    );
  });

  /*
  |--------------------------------------------------------------------------
  | Deriv connection closed
  |--------------------------------------------------------------------------
  */

  derivWs.on("close", () => {
    if (
      browserWs.derivWs === derivWs
    ) {
      browserWs.derivWs = null;
    }

    sendSocket(
      browserWs,
      {
        type: "deriv_status",
        connected: false,
        message:
          "Deriv market-data connection closed."
      }
    );
  });

  /*
  |--------------------------------------------------------------------------
  | Connection status
  |--------------------------------------------------------------------------
  */

  sendSocket(
    browserWs,
    {
      type: "deriv_status",
      connected: true,
      message:
        "Connected to Deriv market data."
    }
  );

  /*
  |--------------------------------------------------------------------------
  | Request historical ticks.
  |--------------------------------------------------------------------------
  |
  | 100 ticks gives the frontend enough data for
  | digit-frequency analysis.
  |--------------------------------------------------------------------------
  */

  const historyRequestId =
    ++browserWs.derivRequestId;

  try {
    derivWs.send(
      JSON.stringify({
        ticks_history:
          browserWs.activeSymbol,

        adjust_start_time: 1,

        count: 100,

        end: "latest",

        start: 1,

        style: "ticks",

        req_id: historyRequestId
      })
    );
  } catch (error) {
    console.error(
      "Unable to request tick history:",
      error.message
    );
  }

  /*
  |--------------------------------------------------------------------------
  | Subscribe to live ticks.
  |--------------------------------------------------------------------------
  */

  const tickRequestId =
    ++browserWs.derivRequestId;

  try {
    derivWs.send(
      JSON.stringify({
        ticks:
          browserWs.activeSymbol,

        subscribe: 1,

        req_id: tickRequestId
      })
    );
  } catch (error) {
    console.error(
      "Unable to subscribe to ticks:",
      error.message
    );
  }
}

/*
|--------------------------------------------------------------------------
| STOP DERIV STREAM
|--------------------------------------------------------------------------
*/

function stopDerivStream(browserWs) {
  if (!browserWs) {
    return;
  }

  const derivWs =
    browserWs.derivWs;

  if (!derivWs) {
    return;
  }

  try {
    /*
    |--------------------------------------------------------------------------
    | Unsubscribe from the tick stream if possible.
    |--------------------------------------------------------------------------
    */

    if (
      derivWs.readyState === WebSocket.OPEN
    ) {
      try {
        derivWs.send(
          JSON.stringify({
            forget_all: "ticks"
          })
        );
      } catch (_) {
        // Ignore.
      }
    }
  } catch (_) {
    // Ignore.
  }

  try {
    derivWs.close();
  } catch (_) {
    // Ignore.
  }

  browserWs.derivWs = null;

  browserWs.activeSymbol = null;

  browserWs.activeMarketName = null;
}

/*
|--------------------------------------------------------------------------
| BROWSER WEBSOCKET MESSAGE HANDLER
|--------------------------------------------------------------------------
*/

wsServer.on(
  "connection",
  (browserWs, req) => {
    console.log(
      "Browser WebSocket connected:",
      req.socket.remoteAddress
    );

    browserWs.isAlive = true;

    browserWs.derivWs = null;

    browserWs.activeSymbol = null;

    browserWs.activeMarketName = null;

    browserWs.activePrecision = 2;

    browserWs.activeContractType = null;

    browserWs.derivRequestId = 2000;

    /*
    |--------------------------------------------------------------------------
    | Pong
    |--------------------------------------------------------------------------
    */

    browserWs.on("pong", () => {
      browserWs.isAlive = true;
    });

    /*
    |--------------------------------------------------------------------------
    | Browser messages
    |--------------------------------------------------------------------------
    */

    browserWs.on("message", async (rawMessage) => {
      let data;

      try {
        const text =
          Buffer.isBuffer(rawMessage)
            ? rawMessage.toString("utf8")
            : String(rawMessage);

        data = JSON.parse(text);
      } catch (error) {
        sendSocket(
          browserWs,
          {
            type: "deriv_error",
            message:
              "Invalid WebSocket request."
          }
        );

        return;
      }

      /*
      |--------------------------------------------------------------------------
      | START
      |--------------------------------------------------------------------------
      */

      if (
        data.action === "start"
      ) {
        const selectedSymbol =
          normalizeSymbol(
            data.symbol || ""
          );

        const selectedMarketName =
          String(
            data.marketName || ""
          ).trim();

        const contractType =
          String(
            data.contractType || ""
          )
            .trim()
            .toUpperCase();

        /*
        |--------------------------------------------------------------------------
        | Do NOT allow a fake discovery request.
        |--------------------------------------------------------------------------
        */

        if (
          !selectedSymbol ||
          selectedSymbol ===
            "__DISCOVERY_ONLY__"
        ) {
          sendSocket(
            browserWs,
            {
              type: "deriv_error",
              message:
                "Please select a valid Deriv market first."
            }
          );

          return;
        }

        /*
        |--------------------------------------------------------------------------
        | MATCHES protection
        |--------------------------------------------------------------------------
        */

        const matchesUnlocked =
          req.session &&
          req.session.matchesUnlocked === true;

        if (
          contractType === "MATCHES" &&
          !matchesUnlocked
        ) {
          sendSocket(
            browserWs,
            {
              type: "deriv_error",
              message:
                "MATCHES is locked. Enter the activation code first."
            }
          );

          return;
        }

        /*
        |--------------------------------------------------------------------------
        | Stop previous stream before starting another.
        |--------------------------------------------------------------------------
        */

        stopDerivStream(
          browserWs
        );

        /*
        |--------------------------------------------------------------------------
        | Start exact selected market.
        |--------------------------------------------------------------------------
        */

        try {
          await startDerivStream(
            browserWs,
            {
              selectedSymbol,
              selectedMarketName,
              contractType,
              matchesUnlocked
            }
          );
        } catch (error) {
          console.error(
            "START stream error:",
            error
          );

          sendSocket(
            browserWs,
            {
              type: "deriv_error",
              message:
                error.message ||
                "Unable to start market stream."
            }
          );
        }

        return;
      }

      /*
      |--------------------------------------------------------------------------
      | STOP
      |--------------------------------------------------------------------------
      */

      if (
        data.action === "stop"
      ) {
        stopDerivStream(
          browserWs
        );

        sendSocket(
          browserWs,
          {
            type: "deriv_status",
            connected: false,
            message:
              "Market stream stopped."
          }
        );

        return;
      }

      /*
      |--------------------------------------------------------------------------
      | PING
      |--------------------------------------------------------------------------
      */

      if (
        data.action === "ping"
      ) {
        sendSocket(
          browserWs,
          {
            type: "pong"
          }
        );

        return;
      }
    });

    /*
    |--------------------------------------------------------------------------
    | Browser disconnect
    |--------------------------------------------------------------------------
    */

    browserWs.on("close", () => {
      console.log(
        "Browser WebSocket disconnected."
      );

      stopDerivStream(
        browserWs
      );
    });

    /*
    |--------------------------------------------------------------------------
    | Browser WebSocket error
    |--------------------------------------------------------------------------
    */

    browserWs.on("error", (error) => {
      console.error(
        "Browser WebSocket error:",
        error.message
      );

      stopDerivStream(
        browserWs
      );
    });

    /*
    |--------------------------------------------------------------------------
    | Initial status
    |--------------------------------------------------------------------------
    */

    sendSocket(
      browserWs,
      {
        type: "deriv_status",
        connected: false,
        message:
          "Ready. Select a market and press START."
      }
    );
  }
);

/*
|--------------------------------------------------------------------------
| HTTP -> WEBSOCKET UPGRADE
|--------------------------------------------------------------------------
|
| /live is protected by the same session used by /api/login.
|--------------------------------------------------------------------------
*/

server.on(
  "upgrade",
  (request, socket, head) => {
    const pathname =
      new URL(
        request.url,
        `http://${request.headers.host}`
      ).pathname;

    if (pathname !== "/live") {
      socket.destroy();
      return;
    }

    sessionParser(
      request,
      {},
      () => {
        /*
        |--------------------------------------------------------------------------
        | Require login.
        |--------------------------------------------------------------------------
        */

        if (
          !request.session ||
          request.session.authenticated !== true
        ) {
          socket.write(
            "HTTP/1.1 401 Unauthorized\r\n" +
            "Connection: close\r\n" +
            "\r\n"
          );

          socket.destroy();

          return;
        }

        wsServer.handleUpgrade(
          request,
          socket,
          head,
          (ws) => {
            wsServer.emit(
              "connection",
              ws,
              request
            );
          }
        );
      }
    );
  }
);

/*
|--------------------------------------------------------------------------
| WEBSOCKET HEARTBEAT
|--------------------------------------------------------------------------
|
| Prevent stale browser connections from remaining forever.
|--------------------------------------------------------------------------
*/

const heartbeatInterval =
  setInterval(() => {
    wsServer.clients.forEach(
      (browserWs) => {
        if (
          browserWs.isAlive === false
        ) {
          try {
            stopDerivStream(
              browserWs
            );
          } catch (_) {
            // Ignore.
          }

          try {
            browserWs.terminate();
          } catch (_) {
            // Ignore.
          }

          return;
        }

        browserWs.isAlive = false;

        try {
          browserWs.ping();
        } catch (_) {
          // Ignore.
        }
      }
    );
  }, 30000);

/*
|--------------------------------------------------------------------------
| CLEANUP HEARTBEAT ON SERVER CLOSE
|--------------------------------------------------------------------------
*/

server.on("close", () => {
  clearInterval(
    heartbeatInterval
  );
});

/*
|--------------------------------------------------------------------------
| PROCESS ERROR HANDLING
|--------------------------------------------------------------------------
*/

process.on(
  "uncaughtException",
  (error) => {
    console.error(
      "UNCAUGHT EXCEPTION:",
      error
    );
  }
);

process.on(
  "unhandledRejection",
  (reason) => {
    console.error(
      "UNHANDLED REJECTION:",
      reason
    );
  }
);

/*
|--------------------------------------------------------------------------
| START SERVER
|--------------------------------------------------------------------------
*/

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      "=================================================="
    );

    console.log(
      "DERIV LIVE ENTRY"
    );

    console.log(
      "POWERED BY ELISY 254"
    );

    console.log(
      "=================================================="
    );

    console.log(
      `Server running on port ${PORT}`
    );

    console.log(
      `Environment: ${process.env.NODE_ENV || "development"}`
    );

    console.log(
      `Current Deriv WebSocket: ${DERIV_PUBLIC_WS}`
    );

    console.log(
      `Legacy Deriv WebSocket fallback: ${DERIV_LEGACY_WS}`
    );

    console.log(
      `Market cache TTL: ${MARKET_CACHE_TTL_MS / 1000}s`
    );

    console.log(
      `MATCHES activation: ${MATCHES_CODE ? "configured" : "not configured"}`
    );

    console.log(
      "Automatic trading: DISABLED"
    );

    console.log(
      "=================================================="
    );
  }
);
