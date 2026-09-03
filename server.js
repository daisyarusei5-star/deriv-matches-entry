require("dotenv").config();

const express = require("express");
const session = require("express-session");
const WebSocket = require("ws");
const http = require("http");
const path = require("path");
const fs = require("fs");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 3000);

const PUBLIC_DIR = path.join(__dirname, "public");
const INDEX_FILE = path.join(PUBLIC_DIR, "index.html");

// ----------------------------------------------------
// DERIV ENDPOINTS
// ----------------------------------------------------

const DERIV_PUBLIC_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const DERIV_LEGACY_WS =
  process.env.DERIV_APP_ID
    ? `wss://ws.binaryws.com/websockets/v3?app_id=${encodeURIComponent(
        process.env.DERIV_APP_ID
      )}`
    : null;

// ----------------------------------------------------
// CONFIG
// ----------------------------------------------------

const SESSION_SECRET =
  process.env.SESSION_SECRET || "CHANGE_THIS_SESSION_SECRET";

const LOGIN_MARKET =
  process.env.LOGIN_MARKET || "Market23";

const LOGIN_PASSWORD =
  process.env.LOGIN_PASSWORD || "Trade23";

const MATCHES_CODE =
  process.env.MATCHES_CODE || "19809";

// Cache markets for 15 minutes.
// This is deliberately much longer than before.
const MARKET_CACHE_TTL = 15 * 60 * 1000;

// Don't retry a failed Deriv connection immediately.
const INITIAL_RECONNECT_DELAY = 5000;
const MAX_RECONNECT_DELAY = 120000;

// ----------------------------------------------------
// BASIC MIDDLEWARE
// ----------------------------------------------------

app.set("trust proxy", 1);

app.use(express.json({ limit: "100kb" }));

app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
      maxAge: 24 * 60 * 60 * 1000
    }
  })
);

// ----------------------------------------------------
// STATIC FRONTEND
// ----------------------------------------------------

app.use(
  express.static(PUBLIC_DIR, {
    index: false,
    extensions: ["html"]
  })
);

app.get("/", (req, res) => {
  if (!fs.existsSync(INDEX_FILE)) {
    return res.status(500).send(
      "Frontend error: public/index.html was not found."
    );
  }

  res.sendFile(INDEX_FILE);
});

// ----------------------------------------------------
// AUTH HELPERS
// ----------------------------------------------------

function requireLogin(req, res, next) {
  if (!req.session || !req.session.authenticated) {
    return res.status(401).json({
      ok: false,
      error: "Not authenticated"
    });
  }

  next();
}

function safeText(value, max = 200) {
  if (typeof value !== "string") return "";
  return value.trim().slice(0, max);
}

// ----------------------------------------------------
// LOGIN
// ----------------------------------------------------

app.post("/api/login", (req, res) => {
  const market = safeText(req.body?.market);
  const password = safeText(req.body?.password);

  if (
    market !== LOGIN_MARKET ||
    password !== LOGIN_PASSWORD
  ) {
    return res.status(401).json({
      ok: false,
      error: "Invalid Market or Password"
    });
  }

  req.session.authenticated = true;
  req.session.loginMarket = market;
  req.session.matchesUnlocked = false;

  return res.json({
    ok: true,
    authenticated: true,
    matchesUnlocked: false
  });
});

// ----------------------------------------------------
// SESSION CHECK
// ----------------------------------------------------

app.get("/api/session", (req, res) => {
  res.json({
    ok: true,
    authenticated: !!req.session?.authenticated,
    matchesUnlocked: !!req.session?.matchesUnlocked
  });
});

// ----------------------------------------------------
// LOGOUT
// ----------------------------------------------------

app.post("/api/logout", (req, res) => {
  req.session.destroy(() => {
    res.json({
      ok: true
    });
  });
});

// ----------------------------------------------------
// MATCHES UNLOCK
// ----------------------------------------------------

app.post("/api/unlock-matches", requireLogin, (req, res) => {
  const code = safeText(req.body?.code);

  if (code !== MATCHES_CODE) {
    return res.status(403).json({
      ok: false,
      error: "Invalid activation code"
    });
  }

  req.session.matchesUnlocked = true;

  return res.json({
    ok: true,
    matchesUnlocked: true
  });
});

// ====================================================
// MARKET CACHE
// ====================================================

let marketCache = {
  markets: [],
  updatedAt: 0
};

// Prevent multiple simultaneous discovery calls.
let marketDiscoveryPromise = null;

// Persistent discovery WebSocket.
let marketSocket = null;
let marketSocketState = "offline";

let marketReconnectTimer = null;
let marketReconnectDelay = INITIAL_RECONNECT_DELAY;

let marketRequestId = 1000;

const marketWaiters = new Map();

function nextMarketRequestId() {
  marketRequestId += 1;
  return marketRequestId;
}

// ----------------------------------------------------
// MARKET NORMALIZATION
// ----------------------------------------------------

function normalizeMarket(item) {
  if (!item || typeof item !== "object") {
    return null;
  }

  // New Deriv API field names
  const symbol =
    item.underlying_symbol ||
    item.symbol ||
    "";

  const name =
    item.underlying_symbol_name ||
    item.display_name ||
    symbol;

  const type =
    item.underlying_symbol_type ||
    item.symbol_type ||
    "";

  const market =
    item.market ||
    "";

  const subgroup =
    item.subgroup ||
    "";

  const submarket =
    item.submarket ||
    "";

  const pip =
    Number(
      item.pip_size ??
      item.pip ??
      0
    );

  if (!symbol) {
    return null;
  }

  const searchable = [
    symbol,
    name,
    type,
    market,
    subgroup,
    submarket
  ]
    .join(" ")
    .toLowerCase();

  // Keep the synthetic markets used by the analyzer.
  const isSynthetic =
    searchable.includes("volatility") ||
    searchable.includes("jump");

  if (!isSynthetic) {
    return null;
  }

  // Don't reject a market simply because the new API
  // doesn't always provide the same metadata as the old API.
  if (
    item.exchange_is_open === 0 &&
    item.is_trading_suspended === 1
  ) {
    return null;
  }

  return {
    symbol,
    name,
    type,
    market,
    subgroup,
    submarket,
    pip
  };
}

// ----------------------------------------------------
// SAVE MARKET CACHE
// ----------------------------------------------------

function saveMarkets(list) {
  const clean = Array.isArray(list)
    ? list.filter(Boolean)
    : [];

  clean.sort((a, b) =>
    String(a.name).localeCompare(
      String(b.name),
      undefined,
      {
        numeric: true,
        sensitivity: "base"
      }
    )
  );

  marketCache = {
    markets: clean,
    updatedAt: Date.now()
  };

  console.log(
    `[MARKETS] Cached ${clean.length} markets`
  );

  return clean;
}

// ----------------------------------------------------
// SEND ACTIVE SYMBOLS REQUEST
// ----------------------------------------------------

function requestActiveSymbols() {
  return new Promise((resolve, reject) => {
    if (
      !marketSocket ||
      marketSocket.readyState !== WebSocket.OPEN
    ) {
      return reject(
        new Error("Market WebSocket is not connected")
      );
    }

    const reqId = nextMarketRequestId();

    const timeout = setTimeout(() => {
      marketWaiters.delete(reqId);

      reject(
        new Error(
          "Timed out waiting for active_symbols"
        )
      );
    }, 15000);

    marketWaiters.set(reqId, {
      resolve,
      reject,
      timeout
    });

    // IMPORTANT:
    // New Deriv API does NOT use product_type/basic here.
    marketSocket.send(
      JSON.stringify({
        active_symbols: "brief",
        req_id: reqId
      })
    );

    console.log(
      `[MARKETS] active_symbols request ${reqId}`
    );
  });
}

// ----------------------------------------------------
// PROCESS ACTIVE SYMBOLS
// ----------------------------------------------------

function processActiveSymbols(data) {
  if (
    !data ||
    !Array.isArray(data.active_symbols)
  ) {
    return [];
  }

  const markets = data.active_symbols
    .map(normalizeMarket)
    .filter(Boolean);

  return saveMarkets(markets);
}

// ----------------------------------------------------
// MARKET SOCKET MESSAGE
// ----------------------------------------------------

function handleMarketMessage(raw) {
  let data;

  try {
    data = JSON.parse(raw.toString());
  } catch {
    return;
  }

  if (data.msg_type === "active_symbols") {
    const reqId =
      data.req_id !== undefined
        ? Number(data.req_id)
        : null;

    if (Array.isArray(data.active_symbols)) {
      const markets =
        processActiveSymbols(data);

      if (reqId && marketWaiters.has(reqId)) {
        const waiter =
          marketWaiters.get(reqId);

        clearTimeout(waiter.timeout);
        marketWaiters.delete(reqId);

        waiter.resolve(markets);
      }
    }

    return;
  }

  if (data.error) {
    const reqId =
      data.req_id !== undefined
        ? Number(data.req_id)
        : null;

    console.error(
      "[MARKETS] Deriv error:",
      data.error.code,
      data.error.message
    );

    if (reqId && marketWaiters.has(reqId)) {
      const waiter =
        marketWaiters.get(reqId);

      clearTimeout(waiter.timeout);
      marketWaiters.delete(reqId);

      waiter.reject(
        new Error(
          `${data.error.code || "DERIV_ERROR"}: ${
            data.error.message || "Unknown error"
          }`
        )
      );
    }

    return;
  }
}

// ----------------------------------------------------
// REJECT ALL MARKET WAITERS
// ----------------------------------------------------

function rejectMarketWaiters(error) {
  for (const [reqId, waiter] of marketWaiters) {
    clearTimeout(waiter.timeout);

    waiter.reject(error);

    marketWaiters.delete(reqId);
  }
}

// ----------------------------------------------------
// CONNECT MARKET SOCKET
// ----------------------------------------------------

function connectMarketSocket() {
  if (
    marketSocket &&
    (
      marketSocket.readyState === WebSocket.OPEN ||
      marketSocket.readyState === WebSocket.CONNECTING
    )
  ) {
    return;
  }

  if (marketReconnectTimer) {
    clearTimeout(marketReconnectTimer);
    marketReconnectTimer = null;
  }

  marketSocketState = "connecting";

  console.log(
    "[MARKETS] Connecting to Deriv public WebSocket..."
  );

  const ws = new WebSocket(DERIV_PUBLIC_WS);

  marketSocket = ws;

  ws.on("open", async () => {
    if (marketSocket !== ws) {
      try {
        ws.close();
      } catch {}
      return;
    }

    marketSocketState = "online";
    marketReconnectDelay = INITIAL_RECONNECT_DELAY;

    console.log(
      "[MARKETS] Deriv market connection ONLINE"
    );

    // If cache is still fresh, do NOT make another request.
    if (
      marketCache.markets.length > 0 &&
      Date.now() - marketCache.updatedAt <
        MARKET_CACHE_TTL
    ) {
      console.log(
        "[MARKETS] Existing cache is fresh; no discovery request."
      );
      return;
    }

    try {
      await requestActiveSymbols();

      console.log(
        `[MARKETS] Discovery complete: ${marketCache.markets.length} markets`
      );
    } catch (error) {
      console.error(
        "[MARKETS] Discovery failed:",
        error.message
      );

      // IMPORTANT:
      // Keep old cache if Deriv rejects the request.
      if (marketCache.markets.length > 0) {
        console.log(
          "[MARKETS] Keeping previous market cache."
        );
      }
    }
  });

  ws.on("message", handleMarketMessage);

  ws.on("error", error => {
    console.error(
      "[MARKETS] WebSocket error:",
      error.message
    );
  });

  ws.on("close", (code, reason) => {
    if (marketSocket === ws) {
      marketSocket = null;
    }

    marketSocketState = "offline";

    rejectMarketWaiters(
      new Error(
        `Market WebSocket closed (${code})`
      )
    );

    console.log(
      `[MARKETS] Connection closed (${code})`
    );

    scheduleMarketReconnect();
  });
}

// ----------------------------------------------------
// MARKET RECONNECT WITH BACKOFF
// ----------------------------------------------------

function scheduleMarketReconnect() {
  if (marketReconnectTimer) {
    return;
  }

  const delay = marketReconnectDelay;

  console.log(
    `[MARKETS] Reconnecting in ${Math.round(
      delay / 1000
    )} seconds...`
  );

  marketReconnectTimer = setTimeout(() => {
    marketReconnectTimer = null;

    connectMarketSocket();

    marketReconnectDelay = Math.min(
      marketReconnectDelay * 2,
      MAX_RECONNECT_DELAY
    );
  }, delay);
}

// ----------------------------------------------------
// GET MARKETS
// ----------------------------------------------------

async function getMarkets() {
  const cacheAge =
    Date.now() - marketCache.updatedAt;

  // FAST PATH:
  // Return cached markets immediately.
  if (
    marketCache.markets.length > 0 &&
    cacheAge < MARKET_CACHE_TTL
  ) {
    return marketCache.markets;
  }

  // If discovery is already running, wait for it.
  if (marketDiscoveryPromise) {
    return marketDiscoveryPromise;
  }

  marketDiscoveryPromise = (async () => {
    try {
      // Ensure persistent connection exists.
      connectMarketSocket();

      // Wait for connection if currently connecting.
      const start = Date.now();

      while (
        (!marketSocket ||
          marketSocket.readyState !== WebSocket.OPEN) &&
        Date.now() - start < 12000
      ) {
        await new Promise(resolve =>
          setTimeout(resolve, 250)
        );
      }

      if (
        !marketSocket ||
        marketSocket.readyState !== WebSocket.OPEN
      ) {
        throw new Error(
          "Deriv market connection unavailable"
        );
      }

      const markets =
        await requestActiveSymbols();

      return markets;
    } catch (error) {
      console.error(
        "[MARKETS] getMarkets failed:",
        error.message
      );

      // VERY IMPORTANT:
      // If we have old markets, use them.
      if (marketCache.markets.length > 0) {
        console.log(
          "[MARKETS] Returning stale market cache."
        );

        return marketCache.markets;
      }

      throw error;
    } finally {
      marketDiscoveryPromise = null;
    }
  })();

  return marketDiscoveryPromise;
}

// ====================================================
// API: MARKETS
// ====================================================

app.get(
  "/api/markets",
  requireLogin,
  async (req, res) => {
    try {
      const markets = await getMarkets();

      return res.json({
        ok: true,
        source:
          Date.now() - marketCache.updatedAt <
          MARKET_CACHE_TTL
            ? "cache"
            : "stale-cache",
        updatedAt: marketCache.updatedAt,
        markets
      });
    } catch (error) {
      console.error(
        "[API] /api/markets:",
        error.message
      );

      return res.status(503).json({
        ok: false,
        error:
          "Markets temporarily unavailable. Please wait a few seconds and try again.",
        markets: []
      });
    }
  }
);

// ====================================================
// MARKET VALIDATION
// ====================================================

async function findMarket(symbol, name) {
  const markets = await getMarkets();

  return markets.find(m => {
    if (symbol && m.symbol === symbol) {
      return true;
    }

    if (
      name &&
      (
        m.name === name ||
        m.symbol === name
      )
    ) {
      return true;
    }

    return false;
  });
}

// ====================================================
// LIVE BROWSER WEBSOCKET
// ====================================================

const liveWss = new WebSocket.Server({
  noServer: true
});

// ----------------------------------------------------
// DERIV LIVE CONNECTION
// ----------------------------------------------------

function connectDerivForClient(client, market) {
  let deriv = null;

  let closed = false;

  function cleanup() {
    if (deriv) {
      try {
        deriv.removeAllListeners();
        deriv.close();
      } catch {}
    }

    deriv = null;
  }

  function send(payload) {
    if (
      client.readyState === WebSocket.OPEN
    ) {
      try {
        client.send(
          JSON.stringify(payload)
        );
      } catch {}
    }
  }

  function connect() {
    if (closed) return;

    send({
      type: "deriv_status",
      status: "connecting"
    });

    console.log(
      `[LIVE] Connecting ${market.symbol}`
    );

    deriv = new WebSocket(
      DERIV_PUBLIC_WS
    );

    deriv.on("open", () => {
      if (closed) return;

      send({
        type: "deriv_status",
        status: "connected"
      });

      send({
        type: "market_confirmed",
        symbol: market.symbol,
        name: market.name,
        precision: getPrecision(market.pip)
      });

      // Historical ticks.
      deriv.send(
        JSON.stringify({
          ticks_history: market.symbol,
          count: 100,
          end: "latest",
          style: "ticks",
          req_id: 2001
        })
      );

      // Live ticks.
      deriv.send(
        JSON.stringify({
          ticks: market.symbol,
          subscribe: 1,
          req_id: 2002
        })
      );
    });

    deriv.on("message", raw => {
      if (closed) return;

      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch {
        return;
      }

      if (data.error) {
        send({
          type: "deriv_error",
          code:
            data.error.code ||
            "DERIV_ERROR",
          message:
            data.error.message ||
            "Deriv error"
        });

        return;
      }

      // -------------------------------
      // HISTORY
      // -------------------------------

      if (
        data.msg_type === "history" &&
        data.history
      ) {
        send({
          type: "history",
          symbol: market.symbol,
          precision:
            data.pip_size !== undefined
              ? getPrecision(data.pip_size)
              : getPrecision(market.pip),
          prices: data.history.prices || [],
          times: data.history.times || []
        });

        return;
      }

      // -------------------------------
      // TICK
      // -------------------------------

      if (
        data.msg_type === "tick" &&
        data.tick
      ) {
        // HARD MARKET ISOLATION
        if (
          data.tick.symbol &&
          data.tick.symbol !== market.symbol
        ) {
          return;
        }

        send({
          type: "tick",
          symbol: market.symbol,
          precision:
            data.tick.pip_size !== undefined
              ? getPrecision(
                  data.tick.pip_size
                )
              : getPrecision(market.pip),
          tick: {
            quote: Number(data.tick.quote),
            epoch: Number(data.tick.epoch)
          }
        });

        return;
      }
    });

    deriv.on("error", error => {
      console.error(
        `[LIVE] ${market.symbol} error:`,
        error.message
      );

      send({
        type: "deriv_error",
        code: "WEBSOCKET_ERROR",
        message:
          "Deriv connection error"
      });
    });

    deriv.on("close", () => {
      if (closed) return;

      send({
        type: "deriv_status",
        status: "disconnected"
      });
    });
  }

  function stop() {
    closed = true;

    cleanup();

    send({
      type: "deriv_status",
      status: "stopped"
    });
  }

  connect();

  return {
    stop
  };
}

// ----------------------------------------------------
// PIP -> PRECISION
// ----------------------------------------------------

function getPrecision(pip) {
  const value = Number(pip);

  if (!Number.isFinite(value) || value <= 0) {
    return 2;
  }

  const text = value.toString();

  if (text.includes("e-")) {
    const parts = text.split("e-");
    return Number(parts[1]);
  }

  const decimal = text.split(".")[1];

  return decimal
    ? decimal.length
    : 0;
}

// ====================================================
// SESSION PARSER FOR WEBSOCKET
// ====================================================

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

// IMPORTANT:
// Express session middleware above and WebSocket
// upgrade need to use the same session store.
// For this application, the default MemoryStore is
// acceptable for a single Render instance.

function getSessionFromUpgrade(req, callback) {
  sessionParser(
    req,
    {},
    () => {
      callback(null);
    }
  );
}

// ====================================================
// BROWSER WS UPGRADE
// ====================================================

server.on("upgrade", (req, socket, head) => {
  const pathname =
    new URL(
      req.url,
      `http://${req.headers.host}`
    ).pathname;

  if (pathname !== "/live") {
    socket.destroy();
    return;
  }

  // Use the normal Express session middleware.
  sessionParser(
    req,
    {},
    async () => {
      if (!req.session?.authenticated) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      liveWss.handleUpgrade(
        req,
        socket,
        head,
        ws => {
          liveWss.emit(
            "connection",
            ws,
            req
          );
        }
      );
    }
  );
});

// ====================================================
// LIVE WS CONNECTION
// ====================================================

liveWss.on(
  "connection",
  ws => {
    let liveConnection = null;
    let selectedMarket = null;

    function send(payload) {
      if (
        ws.readyState === WebSocket.OPEN
      ) {
        try {
          ws.send(
            JSON.stringify(payload)
          );
        } catch {}
      }
    }

    send({
      type: "server_ready"
    });

    ws.on("message", async raw => {
      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch {
        send({
          type: "deriv_error",
          code: "INVALID_JSON",
          message: "Invalid message"
        });

        return;
      }

      // -----------------------------------------
      // START
      // -----------------------------------------

      if (data.action === "start") {
        const symbol =
          safeText(data.symbol, 100);

        const marketName =
          safeText(data.marketName, 200);

        if (!symbol) {
          send({
            type: "market_not_found",
            message: "No market selected"
          });

          return;
        }

        try {
          const market =
            await findMarket(
              symbol,
              marketName
            );

          if (!market) {
            send({
              type: "market_not_found",
              symbol,
              message:
                "Selected market is no longer available"
            });

            return;
          }

          // Stop previous stream first.
          if (liveConnection) {
            liveConnection.stop();
            liveConnection = null;
          }

          selectedMarket = market;

          liveConnection =
            connectDerivForClient(
              ws,
              market
            );
        } catch (error) {
          send({
            type: "market_not_found",
            message:
              "Unable to validate market right now"
          });
        }

        return;
      }

      // -----------------------------------------
      // STOP
      // -----------------------------------------

      if (data.action === "stop") {
        if (liveConnection) {
          liveConnection.stop();
          liveConnection = null;
        }

        selectedMarket = null;

        send({
          type: "deriv_status",
          status: "stopped"
        });

        return;
      }

      // -----------------------------------------
      // PING
      // -----------------------------------------

      if (data.action === "ping") {
        send({
          type: "pong",
          time: Date.now()
        });

        return;
      }
    });

    ws.on("close", () => {
      if (liveConnection) {
        liveConnection.stop();
        liveConnection = null;
      }

      selectedMarket = null;
    });

    ws.on("error", () => {
      if (liveConnection) {
        liveConnection.stop();
        liveConnection = null;
      }
    });
  }
);

// ====================================================
// HEALTH
// ====================================================

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "deriv-live-entry",
    time: new Date().toISOString(),

    markets: {
      socket: marketSocketState,
      count: marketCache.markets.length,
      cacheAge:
        marketCache.updatedAt
          ? Date.now() -
            marketCache.updatedAt
          : null
    }
  });
});

// ====================================================
// ERROR HANDLER
// ====================================================

app.use(
  (err, req, res, next) => {
    console.error(
      "[SERVER ERROR]",
      err
    );

    if (res.headersSent) {
      return next(err);
    }

    res.status(500).json({
      ok: false,
      error: "Internal server error"
    });
  }
);

// ====================================================
// START SERVER
// ====================================================

function startupChecks() {
  console.log(
    "----------------------------------------"
  );

  console.log(
    "DERIV LIVE ENTRY SERVER"
  );

  console.log(
    "----------------------------------------"
  );

  console.log(
    "Node:",
    process.version
  );

  console.log(
    "Public directory:",
    PUBLIC_DIR
  );

  console.log(
    "Index file:",
    INDEX_FILE
  );

  console.log(
    "Index exists:",
    fs.existsSync(INDEX_FILE)
  );

  console.log(
    "Deriv public WS:",
    DERIV_PUBLIC_WS
  );

  console.log(
    "Market cache TTL:",
    MARKET_CACHE_TTL / 60000,
    "minutes"
  );

  console.log(
    "----------------------------------------"
  );

  if (!fs.existsSync(PUBLIC_DIR)) {
    console.error(
      "ERROR: public directory does not exist."
    );
  }

  if (!fs.existsSync(INDEX_FILE)) {
    console.error(
      "ERROR: public/index.html does not exist."
    );
  }
}

startupChecks();

// Connect once at startup.
// It will stay alive and reconnect with backoff.
connectMarketSocket();

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `Server listening on port ${PORT}`
    );
  }
);

// ====================================================
// GRACEFUL SHUTDOWN
// ====================================================

function shutdown(signal) {
  console.log(
    `${signal} received. Shutting down...`
  );

  if (marketReconnectTimer) {
    clearTimeout(marketReconnectTimer);
    marketReconnectTimer = null;
  }

  rejectMarketWaiters(
    new Error("Server shutting down")
  );

  if (marketSocket) {
    try {
      marketSocket.close();
    } catch {}
  }

  server.close(() => {
    process.exit(0);
  });

  setTimeout(() => {
    process.exit(0);
  }, 5000);
}

process.on(
  "SIGTERM",
  () => shutdown("SIGTERM")
);

process.on(
  "SIGINT",
  () => shutdown("SIGINT")
);
