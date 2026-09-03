"use strict";

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const http = require("http");
const express = require("express");
const session = require("express-session");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

/* =========================================================
   CONFIG
========================================================= */

const PORT = Number(process.env.PORT || 3000);

const DERIV_APP_ID = String(process.env.DERIV_APP_ID || "").trim();

const SESSION_SECRET =
  process.env.SESSION_SECRET ||
  "ELISY254_CHANGE_THIS_TO_A_LONG_RANDOM_SECRET";

const LOGIN_MARKET =
  String(process.env.LOGIN_MARKET || "Market23").trim();

const LOGIN_PASSWORD =
  String(process.env.LOGIN_PASSWORD || "Trade23");

const MATCHES_CODE =
  String(process.env.MATCHES_CODE || "19809").trim();

/*
 * Your GitHub structure should be:
 *
 * project/
 * ├── server.js
 * ├── package.json
 * ├── .env
 * └── public/
 *     └── index.html
 */

const PUBLIC_DIR = path.join(__dirname, "public");
const INDEX_FILE = path.join(PUBLIC_DIR, "index.html");

/*
 * Current Deriv public market-data endpoint.
 *
 * No authentication is required for market data.
 */
const DERIV_CURRENT_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/*
 * Legacy public market-data endpoint.
 *
 * Used as a fallback if the current endpoint is temporarily
 * unavailable or rate-limited.
 */
const DERIV_LEGACY_URL = DERIV_APP_ID
  ? `wss://ws.binaryws.com/websockets/v3?app_id=${encodeURIComponent(
      DERIV_APP_ID
    )}`
  : "wss://ws.binaryws.com/websockets/v3";

/*
 * Cache active symbols so multiple browsers do not repeatedly
 * hit Deriv's active_symbols endpoint.
 */
const MARKET_CACHE_TTL = 5 * 60 * 1000;

let marketCache = {
  markets: [],
  fetchedAt: 0,
  source: null
};

let marketDiscoveryPromise = null;

/* =========================================================
   BASIC APP SETUP
========================================================= */

app.set("trust proxy", 1);

app.disable("x-powered-by");

app.use(
  express.json({
    limit: "100kb"
  })
);

app.use(
  express.urlencoded({
    extended: false,
    limit: "100kb"
  })
);

/* =========================================================
   SESSION
========================================================= */

const sessionParser = session({
  secret: SESSION_SECRET,

  resave: false,

  saveUninitialized: false,

  cookie: {
    httpOnly: true,

    sameSite: "lax",

    secure: process.env.NODE_ENV === "production",

    maxAge: 24 * 60 * 60 * 1000
  }
});

app.use(sessionParser);

/* =========================================================
   FILE / STATIC SERVING
========================================================= */

if (!fs.existsSync(PUBLIC_DIR)) {
  console.error("");
  console.error("==============================================");
  console.error("ERROR: public folder was not found.");
  console.error("");
  console.error(`Expected folder: ${PUBLIC_DIR}`);
  console.error("");
  console.error("Your project should contain:");
  console.error("public/index.html");
  console.error("==============================================");
  console.error("");
}

if (!fs.existsSync(INDEX_FILE)) {
  console.error("");
  console.error("==============================================");
  console.error("ERROR: public/index.html was not found.");
  console.error("");
  console.error(`Expected file: ${INDEX_FILE}`);
  console.error("");
  console.error("Make sure the filename is exactly:");
  console.error("index.html");
  console.error("");
  console.error("Linux/Render filenames are case-sensitive.");
  console.error("==============================================");
  console.error("");
}

/*
 * Serve CSS, JS, images and index.html assets from /public.
 */
app.use(
  express.static(PUBLIC_DIR, {
    index: false,
    maxAge: process.env.NODE_ENV === "production" ? "1h" : 0
  })
);

/*
 * IMPORTANT:
 *
 * This explicitly serves:
 *
 * public/index.html
 *
 * This fixes the previous:
 *
 * ENOENT ... /src/index
 *
 * error.
 */
app.get("/", (req, res) => {
  if (!fs.existsSync(INDEX_FILE)) {
    return res.status(500).send(
      "Server configuration error: public/index.html was not found."
    );
  }

  return res.sendFile(INDEX_FILE);
});

/* =========================================================
   HELPERS
========================================================= */

function safeSend(ws, payload) {
  try {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
      return true;
    }
  } catch (error) {
    console.error("WebSocket send error:", error.message);
  }

  return false;
}

function normalizeName(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function precisionFromPip(pip) {
  const number = Number(pip);

  if (!Number.isFinite(number) || number <= 0) {
    return 2;
  }

  let value = number;
  let precision = 0;

  while (
    precision < 12 &&
    Math.abs(value - Math.round(value)) > 1e-10
  ) {
    value *= 10;
    precision++;
  }

  return precision;
}

function precisionFromSymbol(symbol) {
  const text = String(symbol || "").toUpperCase();

  /*
   * Most synthetic/volatility indices use a final digit
   * that can be derived from pip size returned by Deriv.
   *
   * This is only a fallback when pip_size/pip is missing.
   */

  if (
    text.includes("1HZ") ||
    text.includes("VOLATILITY") ||
    text.includes("JUMP")
  ) {
    return 2;
  }

  return 2;
}

function normalizeMarket(item) {
  if (!item || typeof item !== "object") {
    return null;
  }

  /*
   * New Deriv API:
   *
   * underlying_symbol
   * underlying_symbol_name
   * pip_size
   *
   * Legacy API:
   *
   * symbol
   * display_name
   * pip
   */

  const symbol = String(
    item.underlying_symbol ||
      item.symbol ||
      ""
  ).trim();

  const name = String(
    item.underlying_symbol_name ||
      item.display_name ||
      item.name ||
      symbol
  ).trim();

  if (!symbol) {
    return null;
  }

  const searchable = [
    symbol,
    name,
    item.market,
    item.submarket,
    item.subgroup,
    item.underlying_symbol_type,
    item.symbol_type
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  /*
   * We only expose Volatility and Jump markets because
   * that is what the DERIV LIVE ENTRY frontend is intended
   * to analyze.
   */

  if (
    !searchable.includes("volatility") &&
    !searchable.includes("jump")
  ) {
    return null;
  }

  /*
   * If Deriv provides these fields, avoid markets that are
   * closed or suspended.
   *
   * If the fields don't exist, we do not reject the market.
   */

  if (
    item.exchange_is_open !== undefined &&
    Number(item.exchange_is_open) === 0
  ) {
    return null;
  }

  if (
    item.is_trading_suspended !== undefined &&
    Number(item.is_trading_suspended) === 1
  ) {
    return null;
  }

  const pipSizeRaw =
    item.pip_size !== undefined
      ? item.pip_size
      : item.pip;

  const pipSize =
    Number.isFinite(Number(pipSizeRaw))
      ? Number(pipSizeRaw)
      : null;

  const precision =
    pipSize !== null
      ? precisionFromPip(pipSize)
      : precisionFromSymbol(symbol);

  return {
    symbol,
    name,

    precision,

    pipSize,

    market: item.market || null,

    submarket: item.submarket || null,

    subgroup: item.subgroup || null,

    exchangeIsOpen:
      item.exchange_is_open !== undefined
        ? Number(item.exchange_is_open)
        : null,

    tradingSuspended:
      item.is_trading_suspended !== undefined
        ? Number(item.is_trading_suspended)
        : null
  };
}

function sortMarkets(markets) {
  return markets.sort((a, b) => {
    return a.name.localeCompare(
      b.name,
      undefined,
      {
        numeric: true,
        sensitivity: "base"
      }
    );
  });
}

/* =========================================================
   AUTH MIDDLEWARE
========================================================= */

function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated === true) {
    return next();
  }

  return res.status(401).json({
    ok: false,
    authenticated: false,
    message: "Authentication required."
  });
}

/* =========================================================
   LOGIN
========================================================= */

app.post("/api/login", (req, res) => {
  try {
    const market = String(
      req.body?.market || ""
    ).trim();

    const password = String(
      req.body?.password || ""
    );

    if (
      market === LOGIN_MARKET &&
      password === LOGIN_PASSWORD
    ) {
      req.session.authenticated = true;

      req.session.loginMarket = market;

      req.session.matchesUnlocked = false;

      return res.json({
        ok: true,

        authenticated: true,

        message: "Login successful."
      });
    }

    return res.status(401).json({
      ok: false,

      authenticated: false,

      message: "Invalid market or password."
    });
  } catch (error) {
    console.error("Login error:", error);

    return res.status(500).json({
      ok: false,

      message: "Login failed."
    });
  }
});

/* =========================================================
   SESSION CHECK
========================================================= */

app.get("/api/session", (req, res) => {
  const authenticated =
    req.session?.authenticated === true;

  return res.json({
    ok: true,

    authenticated,

    matchesUnlocked:
      authenticated &&
      req.session?.matchesUnlocked === true
  });
});

/* =========================================================
   LOGOUT
========================================================= */

app.post("/api/logout", (req, res) => {
  req.session.destroy(() => {
    return res.json({
      ok: true
    });
  });
});

/* =========================================================
   MATCHES ACTIVATION
========================================================= */

app.post(
  "/api/unlock-matches",
  requireAuth,
  (req, res) => {
    const code = String(
      req.body?.code || ""
    ).trim();

    if (code !== MATCHES_CODE) {
      return res.status(403).json({
        ok: false,

        unlocked: false,

        message: "Invalid activation code."
      });
    }

    req.session.matchesUnlocked = true;

    return res.json({
      ok: true,

      unlocked: true,

      message: "MATCHES activated."
    });
  }
);

/* =========================================================
   DERIV MARKET DISCOVERY
========================================================= */

function requestDerivActiveSymbols(
  url,
  legacyMode = false
) {
  return new Promise((resolve, reject) => {
    let derivSocket = null;

    let settled = false;

    const requestId =
      Date.now() +
      Math.floor(Math.random() * 100000);

    const timeoutMs = 15000;

    const timeout = setTimeout(() => {
      finishReject(
        new Error(
          "Deriv market discovery timed out."
        )
      );
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timeout);

      if (derivSocket) {
        derivSocket.removeAllListeners();
      }
    }

    function finishResolve(value) {
      if (settled) return;

      settled = true;

      cleanup();

      try {
        if (
          derivSocket &&
          derivSocket.readyState === WebSocket.OPEN
        ) {
          derivSocket.close();
        }
      } catch (_) {}

      resolve(value);
    }

    function finishReject(error) {
      if (settled) return;

      settled = true;

      cleanup();

      try {
        if (derivSocket) {
          derivSocket.close();
        }
      } catch (_) {}

      reject(error);
    }

    try {
      derivSocket = new WebSocket(url);
    } catch (error) {
      finishReject(error);
      return;
    }

    derivSocket.on("open", () => {
      const request = legacyMode
        ? {
            active_symbols: "brief",

            product_type: "basic",

            req_id: requestId
          }
        : {
            active_symbols: "brief",

            req_id: requestId
          };

      try {
        derivSocket.send(
          JSON.stringify(request)
        );
      } catch (error) {
        finishReject(error);
      }
    });

    derivSocket.on("message", (raw) => {
      let message;

      try {
        message = JSON.parse(
          raw.toString()
        );
      } catch (error) {
        return;
      }

      if (message.error) {
        const error = new Error(
          message.error.message ||
            message.error.code ||
            "Deriv returned an error."
        );

        error.derivCode =
          message.error.code || null;

        finishReject(error);

        return;
      }

      if (
        message.msg_type === "active_symbols" ||
        Array.isArray(message.active_symbols)
      ) {
        finishResolve({
          items: Array.isArray(
            message.active_symbols
          )
            ? message.active_symbols
            : [],

          source: legacyMode
            ? "legacy"
            : "current"
        });
      }
    });

    derivSocket.on(
      "unexpected-response",
      (request, response) => {
        const status =
          response.statusCode;

        if (status === 429) {
          const error = new Error(
            "DERIV_RATE_LIMIT_429"
          );

          error.httpStatus = 429;

          finishReject(error);

          return;
        }

        const error = new Error(
          `Deriv WebSocket HTTP ${status}`
        );

        error.httpStatus = status;

        finishReject(error);
      }
    );

    derivSocket.on("error", (error) => {
      finishReject(error);
    });

    derivSocket.on("close", () => {
      if (!settled) {
        finishReject(
          new Error(
            "Deriv closed the market discovery connection."
          )
        );
      }
    });
  });
}

/* =========================================================
   DISCOVER MARKETS
========================================================= */

async function discoverMarketsFresh() {
  const errors = [];

  /*
   * First try current Deriv market-data API.
   */

  try {
    const result =
      await requestDerivActiveSymbols(
        DERIV_CURRENT_URL,
        false
      );

    const markets = sortMarkets(
      result.items
        .map(normalizeMarket)
        .filter(Boolean)
    );

    if (markets.length > 0) {
      return {
        markets,

        source: result.source
      };
    }

    errors.push(
      "Current API returned no matching Volatility/Jump markets."
    );
  } catch (error) {
    errors.push(
      `Current API: ${error.message}`
    );

    console.error(
      "Current Deriv discovery error:",
      error.message
    );
  }

  /*
   * Fallback to the legacy public endpoint.
   *
   * This is particularly useful if the current endpoint
   * temporarily returns HTTP 429.
   */

  try {
    const result =
      await requestDerivActiveSymbols(
        DERIV_LEGACY_URL,
        true
      );

    const markets = sortMarkets(
      result.items
        .map(normalizeMarket)
        .filter(Boolean)
    );

    if (markets.length > 0) {
      return {
        markets,

        source: result.source
      };
    }

    errors.push(
      "Legacy API returned no matching Volatility/Jump markets."
    );
  } catch (error) {
    errors.push(
      `Legacy API: ${error.message}`
    );

    console.error(
      "Legacy Deriv discovery error:",
      error.message
    );
  }

  throw new Error(
    `Deriv market discovery failed. ${errors.join(
      " | "
    )}`
  );
}

/* =========================================================
   CACHED MARKET LIST
========================================================= */

async function getMarkets() {
  const now = Date.now();

  /*
   * Return cache when it is still valid.
   */

  if (
    marketCache.markets.length > 0 &&
    now - marketCache.fetchedAt <
      MARKET_CACHE_TTL
  ) {
    return {
      markets: marketCache.markets,

      cached: true,

      stale: false,

      source: marketCache.source
    };
  }

  /*
   * Prevent several simultaneous browsers from
   * opening several active_symbols connections.
   */

  if (marketDiscoveryPromise) {
    return marketDiscoveryPromise;
  }

  marketDiscoveryPromise =
    (async () => {
      try {
        const result =
          await discoverMarketsFresh();

        marketCache = {
          markets: result.markets,

          fetchedAt: Date.now(),

          source: result.source
        };

        return {
          markets: result.markets,

          cached: false,

          stale: false,

          source: result.source
        };
      } catch (error) {
        /*
         * If we have an older working market list,
         * keep serving it rather than taking the whole
         * application offline because of a temporary 429.
         */

        if (
          marketCache.markets.length > 0
        ) {
          console.warn(
            "Using stale market cache:",
            error.message
          );

          return {
            markets: marketCache.markets,

            cached: true,

            stale: true,

            source: marketCache.source,

            warning: error.message
          };
        }

        throw error;
      } finally {
        marketDiscoveryPromise = null;
      }
    })();

  return marketDiscoveryPromise;
}

/* =========================================================
   MARKETS API
========================================================= */

app.get(
  "/api/markets",
  requireAuth,
  async (req, res) => {
    try {
      const result =
        await getMarkets();

      return res.json({
        ok: true,

        markets: result.markets,

        cached: result.cached,

        stale: result.stale,

        source: result.source,

        warning: result.warning || null
      });
    } catch (error) {
      console.error(
        "Market discovery error:",
        error
      );

      return res.status(503).json({
        ok: false,

        markets: [],

        message:
          "Unable to load Deriv markets right now.",

        error: error.message
      });
    }
  }
);

/* =========================================================
   LIVE WEBSOCKET SERVER
========================================================= */

const liveWss =
  new WebSocket.Server({
    noServer: true
  });

/* =========================================================
   HTTP -> WEBSOCKET UPGRADE
========================================================= */

server.on("upgrade", (req, socket, head) => {
  let pathname = "/";

  try {
    const parsedUrl =
      new URL(
        req.url,
        `http://${req.headers.host || "localhost"}`
      );

    pathname = parsedUrl.pathname;
  } catch (_) {}

  /*
   * Only /live is accepted.
   */

  if (pathname !== "/live") {
    socket.write(
      "HTTP/1.1 404 Not Found\r\n" +
        "Connection: close\r\n" +
        "\r\n"
    );

    socket.destroy();

    return;
  }

  /*
   * Read the Express session before allowing
   * the browser WebSocket connection.
   */

  sessionParser(
    req,
    {},
    () => {
      if (
        !req.session ||
        req.session.authenticated !== true
      ) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n" +
            "Connection: close\r\n" +
            "\r\n"
        );

        socket.destroy();

        return;
      }

      liveWss.handleUpgrade(
        req,
        socket,
        head,
        (ws) => {
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

/* =========================================================
   DERIV LIVE CONNECTION HELPERS
========================================================= */

function createDerivLiveConnection(
  url
) {
  return new Promise(
    (resolve, reject) => {
      let ws = null;

      let settled = false;

      const timeout = setTimeout(
        () => {
          if (settled) return;

          settled = true;

          try {
            ws?.close();
          } catch (_) {}

          reject(
            new Error(
              "Deriv live connection timed out."
            )
          );
        },
        15000
      );

      try {
        ws = new WebSocket(url);
      } catch (error) {
        clearTimeout(timeout);

        reject(error);

        return;
      }

      ws.once("open", () => {
        if (settled) return;

        settled = true;

        clearTimeout(timeout);

        resolve(ws);
      });

      ws.once(
        "unexpected-response",
        (request, response) => {
          if (settled) return;

          settled = true;

          clearTimeout(timeout);

          const error =
            new Error(
              response.statusCode === 429
                ? "DERIV_RATE_LIMIT_429"
                : `Deriv WebSocket HTTP ${response.statusCode}`
            );

          error.httpStatus =
            response.statusCode;

          try {
            ws.close();
          } catch (_) {}

          reject(error);
        }
      );

      ws.once("error", (error) => {
        if (settled) return;

        settled = true;

        clearTimeout(timeout);

        reject(error);
      });

      ws.once("close", () => {
        if (settled) return;

        settled = true;

        clearTimeout(timeout);

        reject(
          new Error(
            "Deriv closed the connection before it opened."
          )
        );
      });
    }
  );
}

async function connectDerivLive() {
  /*
   * Current endpoint first.
   */

  try {
    const ws =
      await createDerivLiveConnection(
        DERIV_CURRENT_URL
      );

    return {
      ws,

      source: "current"
    };
  } catch (error) {
    console.error(
      "Current Deriv live connection failed:",
      error.message
    );
  }

  /*
   * Legacy endpoint fallback.
   */

  try {
    const ws =
      await createDerivLiveConnection(
        DERIV_LEGACY_URL
      );

    return {
      ws,

      source: "legacy"
    };
  } catch (error) {
    console.error(
      "Legacy Deriv live connection failed:",
      error.message
    );

    throw new Error(
      "Unable to connect to Deriv market data."
    );
  }
}

/* =========================================================
   LIVE CLIENT CONNECTION
========================================================= */

liveWss.on(
  "connection",
  (client, req) => {
    let derivWs = null;

    let activeSymbol = null;

    let activeMarketName = null;

    let activePrecision = 2;

    let started = false;

    let requestCounter =
      Date.now();

    function nextRequestId() {
      requestCounter += 1;

      return requestCounter;
    }

    function closeDerivConnection() {
      if (!derivWs) {
        return;
      }

      try {
        if (
          derivWs.readyState ===
          WebSocket.OPEN
        ) {
          derivWs.close();
        }
      } catch (_) {}

      derivWs = null;
    }

    function sendStatus(status) {
      safeSend(client, {
        type: "deriv_status",

        status
      });
    }

    /*
     * Attach Deriv message handlers.
     */

    function attachDerivHandlers() {
      if (!derivWs) {
        return;
      }

      derivWs.on(
        "message",
        (raw) => {
          let data;

          try {
            data = JSON.parse(
              raw.toString()
            );
          } catch (error) {
            return;
          }

          /*
           * Deriv error.
           */

          if (data.error) {
            safeSend(client, {
              type: "deriv_error",

              code:
                data.error.code ||
                null,

              message:
                data.error.message ||
                "Deriv returned an error."
            });

            return;
          }

          /*
           * Historical ticks.
           */

          if (
            data.msg_type ===
              "history" &&
            data.history
          ) {
            const prices =
              Array.isArray(
                data.history.prices
              )
                ? data.history.prices
                : [];

            const times =
              Array.isArray(
                data.history.times
              )
                ? data.history.times
                : [];

            safeSend(client, {
              type: "history",

              symbol: activeSymbol,

              precision:
                activePrecision,

              prices,

              times
            });

            return;
          }

          /*
           * Live tick.
           */

          if (
            data.msg_type === "tick" &&
            data.tick
          ) {
            const tickSymbol = String(
              data.tick.symbol ||
                data.symbol ||
                ""
            );

            /*
             * IMPORTANT MARKET ISOLATION:
             *
             * Never forward a tick belonging to
             * another symbol.
             */

            if (
              activeSymbol &&
              tickSymbol &&
              tickSymbol !==
                activeSymbol
            ) {
              return;
            }

            const quote =
              Number(
                data.tick.quote
              );

            const epoch =
              Number(
                data.tick.epoch
              );

            if (
              !Number.isFinite(
                quote
              )
            ) {
              return;
            }

            safeSend(client, {
              type: "tick",

              symbol:
                activeSymbol,

              tick: {
                quote,

                epoch:
                  Number.isFinite(
                    epoch
                  )
                    ? epoch
                    : Math.floor(
                        Date.now() /
                          1000
                      )
              },

              precision:
                activePrecision
            });

            return;
          }
        }
      );

      derivWs.on("error", (error) => {
        console.error(
          "Deriv live socket error:",
          error.message
        );

        safeSend(client, {
          type: "deriv_error",

          message:
            "Deriv live data connection error."
        });
      });

      derivWs.on("close", () => {
        if (client.readyState === WebSocket.OPEN) {
          sendStatus(
            started
              ? "disconnected"
              : "stopped"
          );
        }

        derivWs = null;
      });
    }

    /* =====================================================
       START MARKET
    ===================================================== */

    async function startMarket(
      marketName,
      symbol
    ) {
      /*
       * Clean incoming values.
       */

      marketName = String(
        marketName || ""
      ).trim();

      symbol = String(
        symbol || ""
      ).trim();

      if (!marketName || !symbol) {
        safeSend(client, {
          type: "market_not_found",

          message:
            "A valid market and symbol are required."
        });

        return;
      }

      /*
       * Validate the selected market against
       * Deriv active_symbols.
       */

      let marketResult;

      try {
        marketResult =
          await getMarkets();
      } catch (error) {
        safeSend(client, {
          type: "deriv_error",

          message:
            "Unable to verify the selected market."
        });

        return;
      }

      const selectedMarket =
        marketResult.markets.find(
          (market) => {
            const symbolMatches =
              market.symbol === symbol;

            const nameMatches =
              normalizeName(
                market.name
              ) ===
              normalizeName(
                marketName
              );

            return (
              symbolMatches &&
              nameMatches
            );
          }
        );

      /*
       * If exact name matching fails because Deriv
       * changed the display name, the symbol remains
       * the authoritative identifier.
       *
       * We therefore allow an exact symbol match.
       */

      const fallbackMarket =
        marketResult.markets.find(
          (market) =>
            market.symbol === symbol
        );

      const market =
        selectedMarket ||
        fallbackMarket;

      if (!market) {
        safeSend(client, {
          type: "market_not_found",

          message:
            "The selected market is not currently active on Deriv."
        });

        return;
      }

      /*
       * Stop an existing stream first.
       */

      closeDerivConnection();

      started = false;

      activeSymbol =
        market.symbol;

      activeMarketName =
        market.name;

      activePrecision =
        Number.isInteger(
          market.precision
        )
          ? market.precision
          : 2;

      sendStatus("connecting");

      /*
       * Connect to Deriv.
       */

      let connection;

      try {
        connection =
          await connectDerivLive();

        derivWs =
          connection.ws;
      } catch (error) {
        safeSend(client, {
          type: "deriv_error",

          message:
            "Unable to connect to Deriv live market data."
        });

        sendStatus("error");

        return;
      }

      if (!derivWs) {
        safeSend(client, {
          type: "deriv_error",

          message:
            "Deriv connection was not created."
        });

        sendStatus("error");

        return;
      }

      started = true;

      attachDerivHandlers();

      /*
       * Tell frontend exactly which market was
       * confirmed by the server.
       */

      safeSend(client, {
        type: "market_confirmed",

        symbol:
          activeSymbol,

        marketName:
          activeMarketName,

        precision:
          activePrecision,

        pipSize:
          market.pipSize
      });

      sendStatus("connected");

      /*
       * Request historical ticks.
       *
       * Deriv supports ticks_history for historical
       * market data.
       */

      const historyRequest = {
        ticks_history:
          activeSymbol,

        end: "latest",

        count: 100,

        style: "ticks",

        adjust_start_time: 1,

        req_id:
          nextRequestId()
      };

      /*
       * Request live ticks.
       */

      const ticksRequest = {
        ticks:
          activeSymbol,

        subscribe: 1,

        req_id:
          nextRequestId()
      };

      try {
        derivWs.send(
          JSON.stringify(
            historyRequest
          )
        );

        derivWs.send(
          JSON.stringify(
            ticksRequest
          )
        );
      } catch (error) {
        console.error(
          "Failed to send Deriv requests:",
          error.message
        );

        safeSend(client, {
          type: "deriv_error",

          message:
            "Unable to request live market data."
        });

        closeDerivConnection();

        sendStatus("error");
      }
    }

    /* =====================================================
       STOP MARKET
    ===================================================== */

    function stopMarket() {
      started = false;

      closeDerivConnection();

      activeSymbol = null;

      activeMarketName = null;

      activePrecision = 2;

      sendStatus("stopped");
    }

    /* =====================================================
       BROWSER MESSAGES
    ===================================================== */

    client.on("message", async (raw) => {
      let message;

      try {
        message = JSON.parse(
          raw.toString()
        );
      } catch (error) {
        safeSend(client, {
          type: "deriv_error",

          message:
            "Invalid WebSocket message."
        });

        return;
      }

      if (!message || typeof message !== "object") {
        return;
      }

      /*
       * START
       */

      if (
        message.action === "start"
      ) {
        await startMarket(
          message.marketName,
          message.symbol
        );

        return;
      }

      /*
       * STOP
       */

      if (
        message.action === "stop"
      ) {
        stopMarket();

        return;
      }

      /*
       * Optional ping.
       */

      if (
        message.action === "ping"
      ) {
        safeSend(client, {
          type: "pong"
        });

        return;
      }
    });

    /* =====================================================
       BROWSER CLOSE
    ===================================================== */

    client.on("close", () => {
      started = false;

      closeDerivConnection();

      activeSymbol = null;

      activeMarketName = null;
    });

    client.on("error", (error) => {
      console.error(
        "Browser WebSocket error:",
        error.message
      );

      started = false;

      closeDerivConnection();
    });

    /*
     * Initial connection status.
     */

    safeSend(client, {
      type: "deriv_status",

      status: "ready"
    });
  }
);

/* =========================================================
   HEALTH CHECK
========================================================= */

app.get("/health", (req, res) => {
  return res.json({
    ok: true,

    service:
      "DERIV LIVE ENTRY",

    time:
      new Date().toISOString()
  });
});

/* =========================================================
   404 API HANDLER
========================================================= */

app.use("/api", (req, res) => {
  return res.status(404).json({
    ok: false,

    message:
      "API endpoint not found."
  });
});

/* =========================================================
   GENERAL ERROR HANDLER
========================================================= */

app.use(
  (error, req, res, next) => {
    console.error(
      "Express error:",
      error
    );

    if (res.headersSent) {
      return next(error);
    }

    return res.status(500).json({
      ok: false,

      message:
        "Internal server error."
    });
  }
);

/* =========================================================
   START SERVER
========================================================= */

server.listen(PORT, "0.0.0.0", () => {
  console.log("");
  console.log("==============================================");
  console.log("       DERIV LIVE ENTRY");
  console.log("       POWERED BY ELISY 254");
  console.log("==============================================");
  console.log("");
  console.log(`Server listening on port ${PORT}`);
  console.log("");
  console.log(`Public directory: ${PUBLIC_DIR}`);
  console.log(`Index file:       ${INDEX_FILE}`);
  console.log("");
  console.log(
    `Deriv current:    ${DERIV_CURRENT_URL}`
  );
  console.log(
    `Deriv legacy:     ${DERIV_LEGACY_URL}`
  );
  console.log("");
  console.log(
    "Automatic trading: DISABLED"
  );
  console.log(
    "Buy requests:      DISABLED"
  );
  console.log(
    "Analysis only:     ENABLED"
  );
  console.log("");
  console.log("==============================================");
  console.log("");
});
