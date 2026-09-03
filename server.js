const express = require("express");
const session = require("express-session");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");
require("dotenv").config();

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 3000);

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/*
|--------------------------------------------------------------------------
| Configuration
|--------------------------------------------------------------------------
*/

const SESSION_SECRET =
  process.env.SESSION_SECRET;

const LOGIN_MARKET =
  process.env.LOGIN_MARKET || "Market23";

const LOGIN_PASSWORD =
  process.env.LOGIN_PASSWORD || "Trade23";

const MATCHES_CODE =
  process.env.MATCHES_CODE || "19809";

if (!SESSION_SECRET) {
  console.error(
    "[CONFIG] SESSION_SECRET is missing."
  );

  process.exit(1);
}

/*
|--------------------------------------------------------------------------
| Express
|--------------------------------------------------------------------------
*/

app.set(
  "trust proxy",
  process.env.NODE_ENV === "production" ? 1 : 0
);

app.use(express.json());

const sessionMiddleware = session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,

  cookie: {
    httpOnly: true,
    sameSite: "lax",
    secure:
      process.env.NODE_ENV === "production",
    maxAge: 1000 * 60 * 60 * 12
  }
});

app.use(sessionMiddleware);

app.use(express.static("public"));

/*
|--------------------------------------------------------------------------
| Login
|--------------------------------------------------------------------------
*/

app.post("/api/login", (req, res) => {
  const { market, password } = req.body || {};

  if (
    String(market || "") !==
      String(LOGIN_MARKET) ||
    String(password || "") !==
      String(LOGIN_PASSWORD)
  ) {
    return res.status(401).json({
      ok: false,
      message: "Wrong Market or Password."
    });
  }

  req.session.loggedIn = true;
  req.session.matchesUnlocked = false;

  req.session.save((err) => {
    if (err) {
      console.error(
        "[SESSION] Login save error:",
        err
      );

      return res.status(500).json({
        ok: false,
        message: "Could not create login session."
      });
    }

    res.json({
      ok: true
    });
  });
});

/*
|--------------------------------------------------------------------------
| Session check
|--------------------------------------------------------------------------
*/

app.get("/api/session", (req, res) => {
  res.json({
    loggedIn: !!req.session.loggedIn,
    matchesUnlocked:
      !!req.session.matchesUnlocked
  });
});

/*
|--------------------------------------------------------------------------
| Logout
|--------------------------------------------------------------------------
*/

app.post("/api/logout", (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error(
        "[SESSION] Logout error:",
        err
      );

      return res.status(500).json({
        ok: false
      });
    }

    res.clearCookie("connect.sid");

    res.json({
      ok: true
    });
  });
});

/*
|--------------------------------------------------------------------------
| MATCHES unlock
|--------------------------------------------------------------------------
*/

app.post("/api/unlock-matches", (req, res) => {
  if (!req.session.loggedIn) {
    return res.status(401).json({
      ok: false,
      message: "Please login first."
    });
  }

  const { code } = req.body || {};

  if (
    String(code || "") !==
    String(MATCHES_CODE)
  ) {
    return res.status(403).json({
      ok: false,
      message: "Invalid activation code."
    });
  }

  req.session.matchesUnlocked = true;

  req.session.save((err) => {
    if (err) {
      return res.status(500).json({
        ok: false,
        message:
          "Could not save activation state."
      });
    }

    res.json({
      ok: true,
      message: "MATCHES unlocked."
    });
  });
});

/*
|--------------------------------------------------------------------------
| WebSocket one-time authentication tokens
|--------------------------------------------------------------------------
|
| The browser first calls:
|
|   GET /api/ws-token
|
| while logged in.
|
| The server returns a short-lived token.
|
| Browser then connects to:
|
|   /ws?token=xxxxx
|
|--------------------------------------------------------------------------
*/

const wsTokens = new Map();

const WS_TOKEN_LIFETIME = 60 * 1000;

app.get("/api/ws-token", (req, res) => {
  if (!req.session.loggedIn) {
    return res.status(401).json({
      ok: false,
      message: "Please login first."
    });
  }

  const token = crypto
    .randomBytes(32)
    .toString("hex");

  wsTokens.set(token, {
    expiresAt:
      Date.now() + WS_TOKEN_LIFETIME
  });

  res.json({
    ok: true,
    token
  });
});

/*
|--------------------------------------------------------------------------
| Cleanup expired WS tokens
|--------------------------------------------------------------------------
*/

setInterval(() => {
  const now = Date.now();

  for (const [token, info] of wsTokens) {
    if (info.expiresAt <= now) {
      wsTokens.delete(token);
    }
  }
}, 30 * 1000).unref();

/*
|--------------------------------------------------------------------------
| Deriv shared WebSocket
|--------------------------------------------------------------------------
*/

let derivSocket = null;
let derivConnecting = false;
let derivReconnectTimer = null;

let derivBackoff = 2000;

let requestId = 1;

let activeSymbols = [];
let activeSymbolsLoadedAt = 0;

const ACTIVE_SYMBOL_CACHE_MS =
  10 * 60 * 1000;

/*
|--------------------------------------------------------------------------
| Browser clients
|--------------------------------------------------------------------------
*/

const browserClients = new Set();

/*
|--------------------------------------------------------------------------
| Deriv subscriptions
|--------------------------------------------------------------------------
|
| symbol -> {
|   symbol,
|   subscriptionId,
|   requestId,
|   clients: Set<WebSocket>
| }
|--------------------------------------------------------------------------
*/

const symbolSubscriptions =
  new Map();

/*
|--------------------------------------------------------------------------
| Pending Deriv requests
|--------------------------------------------------------------------------
*/

const pendingRequests =
  new Map();

/*
|--------------------------------------------------------------------------
| Utility
|--------------------------------------------------------------------------
*/

function nextRequestId() {
  return requestId++;
}

function sendDeriv(payload) {
  if (
    !derivSocket ||
    derivSocket.readyState !==
      WebSocket.OPEN
  ) {
    return false;
  }

  try {
    derivSocket.send(
      JSON.stringify(payload)
    );

    return true;
  } catch (err) {
    console.error(
      "[DERIV] Send error:",
      err.message
    );

    return false;
  }
}

function sendClient(client, payload) {
  if (
    client &&
    client.readyState ===
      WebSocket.OPEN
  ) {
    try {
      client.send(
        JSON.stringify(payload)
      );
    } catch {}
  }
}

/*
|--------------------------------------------------------------------------
| Broadcast to browser clients
|--------------------------------------------------------------------------
*/

function broadcast(payload) {
  for (const client of browserClients) {
    sendClient(client, payload);
  }
}

/*
|--------------------------------------------------------------------------
| Market payload
|--------------------------------------------------------------------------
*/

function marketPayload() {
  return {
    type: "markets",

    markets: activeSymbols.map(
      (market) => ({
        name: market.name,
        symbol: market.symbol,
        pipSize: market.pipSize,
        precision:
          precisionFromPipSize(
            market.pipSize
          )
      })
    )
  };
}

function broadcastMarkets() {
  broadcast(marketPayload());
}

/*
|--------------------------------------------------------------------------
| Connect Deriv
|--------------------------------------------------------------------------
*/

function connectDeriv() {
  if (derivConnecting) {
    return;
  }

  if (
    derivSocket &&
    derivSocket.readyState ===
      WebSocket.OPEN
  ) {
    return;
  }

  if (derivReconnectTimer) {
    clearTimeout(
      derivReconnectTimer
    );

    derivReconnectTimer = null;
  }

  derivConnecting = true;

  console.log(
    "[DERIV] Connecting..."
  );

  const ws = new WebSocket(
    DERIV_WS_URL
  );

  derivSocket = ws;

  ws.on("open", async () => {
    derivConnecting = false;
    derivBackoff = 2000;

    console.log(
      "[DERIV] Connected."
    );

    try {
      await loadActiveSymbols(true);

      console.log(
        `[DERIV] Active markets: ${activeSymbols.length}`
      );

      broadcastMarkets();

      /*
       * Re-subscribe all active browser
       * subscriptions after reconnect.
       */
      resubscribeAll();
    } catch (err) {
      console.error(
        "[DERIV] Startup error:",
        err.message
      );
    }
  });

  ws.on("message", (raw) => {
    handleDerivMessage(raw);
  });

  ws.on("error", (err) => {
    console.error(
      "[DERIV] WebSocket error:",
      err.message
    );
  });

  ws.on("close", (code, reason) => {
    derivConnecting = false;

    if (derivSocket === ws) {
      derivSocket = null;
    }

    /*
     * The old Deriv subscription IDs are
     * no longer reliable after reconnect.
     */
    for (const subscription of
      symbolSubscriptions.values()) {
      subscription.subscriptionId =
        null;
      subscription.requestId = null;
    }

    console.log(
      `[DERIV] Closed. code=${code} reason=${reason ? reason.toString() : ""}`
    );

    broadcast({
      type: "deriv_reconnecting"
    });

    scheduleDerivReconnect();
  });
}

/*
|--------------------------------------------------------------------------
| Reconnect with exponential backoff
|--------------------------------------------------------------------------
*/

function scheduleDerivReconnect() {
  if (derivReconnectTimer) {
    return;
  }

  const delay = Math.min(
    derivBackoff,
    60 * 1000
  );

  console.log(
    `[DERIV] Reconnecting in ${Math.round(delay / 1000)}s...`
  );

  derivReconnectTimer = setTimeout(() => {
    derivReconnectTimer = null;

    connectDeriv();

    derivBackoff = Math.min(
      derivBackoff * 2,
      60 * 1000
    );
  }, delay);
}

/*
|--------------------------------------------------------------------------
| Re-subscribe active symbols
|--------------------------------------------------------------------------
*/

function resubscribeAll() {
  if (
    !derivSocket ||
    derivSocket.readyState !==
      WebSocket.OPEN
  ) {
    return;
  }

  for (
    const [symbol, subscription] of
      symbolSubscriptions
  ) {
    if (
      subscription.clients.size === 0
    ) {
      continue;
    }

    requestDerivSubscription(
      symbol,
      subscription
    );
  }
}

/*
|--------------------------------------------------------------------------
| Subscribe symbol on Deriv
|--------------------------------------------------------------------------
*/

function requestDerivSubscription(
  symbol,
  subscription
) {
  if (
    !derivSocket ||
    derivSocket.readyState !==
      WebSocket.OPEN
  ) {
    return false;
  }

  const reqId = nextRequestId();

  subscription.requestId = reqId;
  subscription.subscriptionId = null;

  const sent = sendDeriv({
    ticks: symbol,
    subscribe: 1,
    req_id: reqId
  });

  if (!sent) {
    subscription.requestId = null;
  }

  return sent;
}

/*
|--------------------------------------------------------------------------
| Handle Deriv messages
|--------------------------------------------------------------------------
*/

function handleDerivMessage(raw) {
  let data;

  try {
    data = JSON.parse(
      raw.toString()
    );
  } catch {
    return;
  }

  /*
   * Active symbols
   */
  if (
    data.msg_type ===
    "active_symbols"
  ) {
    const list =
      Array.isArray(
        data.active_symbols
      )
        ? data.active_symbols
        : [];

    activeSymbols = list
      .filter((item) => {
        const name = String(
          item.underlying_symbol_name ||
            ""
        );

        return /volatility|jump/i.test(
          name
        );
      })
      .filter(
        (item) =>
          item.underlying_symbol
      )
      .map((item) => ({
        symbol:
          item.underlying_symbol,

        name:
          item.underlying_symbol_name,

        pipSize:
          Number(
            item.pip_size || 0
          ),

        market:
          item.market || "",

        subgroup:
          item.subgroup || "",

        submarket:
          item.submarket || "",

        exchangeOpen:
          item.exchange_is_open !==
          0,

        suspended:
          item.is_trading_suspended ===
          1
      }))
      .filter(
        (item) => !item.suspended
      )
      .sort((a, b) =>
        a.name.localeCompare(
          b.name,
          undefined,
          {
            numeric: true,
            sensitivity: "base"
          }
        )
      );

    activeSymbolsLoadedAt =
      Date.now();

    resolveRequest(
      data.req_id,
      data
    );

    broadcastMarkets();

    return;
  }

  /*
   * Tick
   */
  if (
    data.msg_type === "tick" &&
    data.tick
  ) {
    const tick = data.tick;

    const symbol =
      tick.symbol;

    if (!symbol) {
      return;
    }

    const subscription =
      symbolSubscriptions.get(
        symbol
      );

    if (!subscription) {
      return;
    }

    /*
     * IMPORTANT:
     * Capture the Deriv subscription ID.
     */
    if (
      data.subscription &&
      data.subscription.id
    ) {
      subscription.subscriptionId =
        data.subscription.id;

      /*
       * If everyone stopped while the
       * subscribe request was pending,
       * immediately forget it.
       */
      if (
        subscription.clients.size ===
        0
      ) {
        forgetSubscription(
          symbol,
          subscription
        );

        symbolSubscriptions.delete(
          symbol
        );

        return;
      }
    }

    const market =
      activeSymbols.find(
        (item) =>
          item.symbol === symbol
      );

    const pipSize =
      Number(
        tick.pip_size || 0
      ) ||
      (market
        ? Number(
            market.pipSize || 0
          )
        : 0);

    const payload = {
      type: "tick",

      market: market
        ? market.name
        : symbol,

      symbol,

      quote: tick.quote,

      epoch: tick.epoch,

      pipSize,

      precision: market
        ? precisionFromPipSize(
            market.pipSize
          )
        : precisionFromPipSize(
            tick.pip_size
          )
    };

    for (const client of
      subscription.clients) {
      sendClient(
        client,
        payload
      );
    }

    return;
  }

  /*
   * Errors
   */
  if (data.error) {
    console.error(
      "[DERIV] API error:",
      data.error.code,
      data.error.message
    );

    resolveRequest(
      data.req_id,
      data
    );

    /*
     * Find subscription associated
     * with failed request.
     */
    for (
      const [
        symbol,
        subscription
      ] of symbolSubscriptions
    ) {
      if (
        subscription.requestId ===
        data.req_id
      ) {
        subscription.requestId =
          null;

        for (const client of
          subscription.clients) {
          sendClient(client, {
            type: "deriv_error",

            message:
              data.error.message ||
              "Deriv market data error."
          });
        }

        break;
      }
    }

    /*
     * Also support errors containing
     * echo_req.ticks.
     */
    if (
      data.echo_req &&
      data.echo_req.ticks
    ) {
      const symbol =
        data.echo_req.ticks;

      const subscription =
        symbolSubscriptions.get(
          symbol
        );

      if (subscription) {
        for (const client of
          subscription.clients) {
          sendClient(client, {
            type: "deriv_error",

            message:
              data.error.message ||
              "Deriv market data error."
          });
        }
      }
    }

    return;
  }

  resolveRequest(
    data.req_id,
    data
  );
}

/*
|--------------------------------------------------------------------------
| Resolve request
|--------------------------------------------------------------------------
*/

function resolveRequest(
  reqId,
  data
) {
  if (!reqId) {
    return;
  }

  const resolver =
    pendingRequests.get(
      reqId
    );

  if (!resolver) {
    return;
  }

  pendingRequests.delete(
    reqId
  );

  resolver(data);
}

/*
|--------------------------------------------------------------------------
| Active symbols
|--------------------------------------------------------------------------
*/

let activeSymbolsRequest = null;

function loadActiveSymbols(
  force = false
) {
  const fresh =
    Date.now() -
      activeSymbolsLoadedAt <
    ACTIVE_SYMBOL_CACHE_MS;

  if (
    !force &&
    fresh &&
    activeSymbols.length
  ) {
    return Promise.resolve(
      activeSymbols
    );
  }

  if (activeSymbolsRequest) {
    return activeSymbolsRequest;
  }

  activeSymbolsRequest =
    new Promise(
      (resolve, reject) => {
        if (
          !derivSocket ||
          derivSocket.readyState !==
            WebSocket.OPEN
        ) {
          activeSymbolsRequest =
            null;

          return reject(
            new Error(
              "Deriv WebSocket is not connected."
            )
          );
        }

        const reqId =
          nextRequestId();

        const timer =
          setTimeout(() => {
            pendingRequests.delete(
              reqId
            );

            reject(
              new Error(
                "Timed out waiting for active_symbols."
              )
            );
          }, 15000);

        pendingRequests.set(
          reqId,
          (data) => {
            clearTimeout(timer);

            if (data.error) {
              reject(
                new Error(
                  data.error.message ||
                    "active_symbols failed."
                )
              );

              return;
            }

            resolve(
              activeSymbols
            );
          }
        );

        const sent =
          sendDeriv({
            active_symbols:
              "brief",

            req_id: reqId
          });

        if (!sent) {
          clearTimeout(timer);

          pendingRequests.delete(
            reqId
          );

          reject(
            new Error(
              "Could not request active markets."
            )
          );
        }
      }
    ).finally(() => {
      activeSymbolsRequest =
        null;
    });

  return activeSymbolsRequest;
}

/*
|--------------------------------------------------------------------------
| Precision
|--------------------------------------------------------------------------
*/

function precisionFromPipSize(
  pipSize
) {
  const value =
    Number(pipSize);

  if (
    !Number.isFinite(value) ||
    value <= 0
  ) {
    return null;
  }

  const decimals =
    Math.round(
      -Math.log10(value)
    );

  return Math.max(
    0,
    Math.min(decimals, 10)
  );
}

/*
|--------------------------------------------------------------------------
| Market name normalization
|--------------------------------------------------------------------------
*/

function normalizeMarketName(
  value
) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

/*
|--------------------------------------------------------------------------
| Exact market resolver
|--------------------------------------------------------------------------
*/

function resolveExactMarket(
  marketName
) {
  const requested =
    normalizeMarketName(
      marketName
    );

  let found =
    activeSymbols.find(
      (item) =>
        normalizeMarketName(
          item.name
        ) === requested
    );

  if (found) {
    return found;
  }

  const compact =
    requested
      .replace(/[–—]/g, "-")
      .replace(/\s+/g, "");

  found =
    activeSymbols.find(
      (item) => {
        const candidate =
          normalizeMarketName(
            item.name
          )
            .replace(
              /[–—]/g,
              "-"
            )
            .replace(
              /\s+/g,
              ""
            );

        return (
          candidate === compact
        );
      }
    );

  return found || null;
}

/*
|--------------------------------------------------------------------------
| Subscribe browser client
|--------------------------------------------------------------------------
*/

async function subscribeClientToMarket(
  client,
  marketName
) {
  try {
    await loadActiveSymbols(
      false
    );
  } catch (err) {
    sendClient(client, {
      type: "market_error",
      message: err.message
    });

    return;
  }

  const market =
    resolveExactMarket(
      marketName
    );

  if (!market) {
    sendClient(client, {
      type: "market_error",
      message:
        "That market is not currently active on Deriv."
    });

    return;
  }

  /*
   * Remove old market first.
   */
  unsubscribeClientFromAllMarkets(
    client
  );

  let subscription =
    symbolSubscriptions.get(
      market.symbol
    );

  if (!subscription) {
    subscription = {
      symbol: market.symbol,
      subscriptionId: null,
      requestId: null,
      clients: new Set()
    };

    symbolSubscriptions.set(
      market.symbol,
      subscription
    );

    /*
     * Create one Deriv subscription
     * for this symbol.
     */
    requestDerivSubscription(
      market.symbol,
      subscription
    );
  }

  subscription.clients.add(
    client
  );

  client.currentSymbol =
    market.symbol;

  client.currentMarket =
    market.name;

  sendClient(client, {
    type: "market_connected",

    market: market.name,

    symbol: market.symbol,

    pipSize: market.pipSize,

    precision:
      precisionFromPipSize(
        market.pipSize
      )
  });
}

/*
|--------------------------------------------------------------------------
| Remove client subscriptions
|--------------------------------------------------------------------------
*/

function unsubscribeClientFromAllMarkets(
  client
) {
  for (
    const [
      symbol,
      subscription
    ] of symbolSubscriptions
  ) {
    if (
      subscription.clients.has(
        client
      )
    ) {
      subscription.clients.delete(
        client
      );

      if (
        subscription.clients.size ===
        0
      ) {
        /*
         * Only forget when nobody is
         * using the symbol.
         */
        forgetSubscription(
          symbol,
          subscription
        );

        symbolSubscriptions.delete(
          symbol
        );
      }
    }
  }

  client.currentSymbol = null;
  client.currentMarket = null;
}

/*
|--------------------------------------------------------------------------
| Forget Deriv subscription
|--------------------------------------------------------------------------
*/

function forgetSubscription(
  symbol,
  subscription
) {
  if (
    !subscription
  ) {
    return;
  }

  if (
    subscription.subscriptionId &&
    derivSocket &&
    derivSocket.readyState ===
      WebSocket.OPEN
  ) {
    sendDeriv({
      forget:
        subscription.subscriptionId
    });

    console.log(
      `[DERIV] Forgot subscription for ${symbol}`
    );
  }

  subscription.subscriptionId =
    null;

  subscription.requestId =
    null;
}

/*
|--------------------------------------------------------------------------
| Browser WebSocket
|--------------------------------------------------------------------------
*/

const wss =
  new WebSocket.Server({
    noServer: true
  });

/*
|--------------------------------------------------------------------------
| Secure WebSocket upgrade
|--------------------------------------------------------------------------
*/

server.on(
  "upgrade",
  (request, socket, head) => {
    try {
      const url =
        new URL(
          request.url,
          `http://${request.headers.host}`
        );

      /*
       * Only allow our /ws endpoint.
       */
      if (url.pathname !== "/ws") {
        socket.write(
          "HTTP/1.1 404 Not Found\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      const token =
        url.searchParams.get(
          "token"
        );

      if (!token) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      const tokenInfo =
        wsTokens.get(token);

      /*
       * One-time token.
       */
      wsTokens.delete(token);

      if (
        !tokenInfo ||
        tokenInfo.expiresAt <=
          Date.now()
      ) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      wss.handleUpgrade(
        request,
        socket,
        head,
        (client) => {
          client.authenticated =
            true;

          wss.emit(
            "connection",
            client,
            request
          );
        }
      );
    } catch (err) {
      console.error(
        "[WS] Upgrade error:",
        err.message
      );

      socket.destroy();
    }
  }
);

/*
|--------------------------------------------------------------------------
| Browser connection
|--------------------------------------------------------------------------
*/

wss.on(
  "connection",
  (client) => {
    browserClients.add(client);

    client.currentSymbol =
      null;

    client.currentMarket =
      null;

    /*
     * Make sure Deriv is alive.
     */
    connectDeriv();

    sendClient(client, {
      type: "hello",
      authenticated: true
    });

    /*
     * Send markets immediately if cached.
     */
    if (activeSymbols.length) {
      sendClient(
        client,
        marketPayload()
      );
    }

    /*
     * Browser requests
     */
    client.on(
      "message",
      async (raw) => {
        if (
          !client.authenticated
        ) {
          client.close(
            1008,
            "Unauthorized"
          );

          return;
        }

        let data;

        try {
          data = JSON.parse(
            raw.toString()
          );
        } catch {
          sendClient(client, {
            type: "error",
            message:
              "Invalid message."
          });

          return;
        }

        /*
         * Markets
         */
        if (
          data.action ===
          "markets"
        ) {
          try {
            await loadActiveSymbols(
              false
            );

            sendClient(
              client,
              marketPayload()
            );
          } catch (err) {
            sendClient(client, {
              type: "market_error",
              message:
                err.message
            });
          }

          return;
        }

        /*
         * Start
         */
        if (
          data.action === "start"
        ) {
          const marketName =
            String(
              data.marketName ||
                ""
            ).trim();

          if (!marketName) {
            sendClient(client, {
              type: "market_error",
              message:
                "Please select a market."
            });

            return;
          }

          await subscribeClientToMarket(
            client,
            marketName
          );

          return;
        }

        /*
         * Stop
         */
        if (
          data.action === "stop"
        ) {
          unsubscribeClientFromAllMarkets(
            client
          );

          sendClient(client, {
            type: "market_stopped"
          });

          return;
        }

        /*
         * Unknown action
         */
        sendClient(client, {
          type: "error",
          message:
            "Unknown WebSocket action."
        });
      }
    );

    /*
     * Connection closed
     */
    client.on("close", () => {
      unsubscribeClientFromAllMarkets(
        client
      );

      browserClients.delete(
        client
      );
    });

    client.on("error", () => {
      unsubscribeClientFromAllMarkets(
        client
      );

      browserClients.delete(
        client
      );
    });
  }
);

/*
|--------------------------------------------------------------------------
| Pending request cleanup
|--------------------------------------------------------------------------
*/

setInterval(() => {
  /*
   * Prevent the map from growing forever
   * if an upstream request disappears.
   *
   * Individual requests already have their
   * own timeout, so this is just a safety net.
   */
  if (
    pendingRequests.size >
    1000
  ) {
    console.warn(
      `[DERIV] Large pending request count: ${pendingRequests.size}`
    );
  }
}, 60 * 1000).unref();

/*
|--------------------------------------------------------------------------
| Start server
|--------------------------------------------------------------------------
*/

server.listen(
  PORT,
  () => {
    console.log(
      `DERIV LIVE ENTRY running on port ${PORT}`
    );

    connectDeriv();
  }
);
