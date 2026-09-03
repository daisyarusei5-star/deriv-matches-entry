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
        err.message
      );

      return res.status(500).json({
        ok: false,
        message:
          "Could not create login session."
      });
    }

    res.json({
      ok: true
    });
  });
});

/*
|--------------------------------------------------------------------------
| Session
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
        err.message
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
| WebSocket one-time authentication
|--------------------------------------------------------------------------
*/

const wsTokens = new Map();

const WS_TOKEN_LIFETIME =
  60 * 1000;

app.get("/api/ws-token", (req, res) => {
  if (!req.session.loggedIn) {
    return res.status(401).json({
      ok: false,
      message: "Please login first."
    });
  }

  const token =
    crypto
      .randomBytes(32)
      .toString("hex");

  wsTokens.set(token, {
    expiresAt:
      Date.now() +
      WS_TOKEN_LIFETIME
  });

  res.json({
    ok: true,
    token
  });
});

/*
|--------------------------------------------------------------------------
| Cleanup expired WebSocket tokens
|--------------------------------------------------------------------------
*/

setInterval(() => {
  const now = Date.now();

  for (
    const [token, info] of wsTokens
  ) {
    if (
      info.expiresAt <= now
    ) {
      wsTokens.delete(token);
    }
  }
}, 30 * 1000).unref();

/*
|--------------------------------------------------------------------------
| Browser clients
|--------------------------------------------------------------------------
*/

const browserClients =
  new Set();

/*
|--------------------------------------------------------------------------
| Deriv connection manager
|--------------------------------------------------------------------------
*/

let derivSocket = null;
let derivConnecting = false;
let derivReconnectTimer = null;

let derivBackoff = 30000;
let derivLastAttempt = 0;

const DERIV_MAX_BACKOFF =
  5 * 60 * 1000;

const DERIV_MIN_CONNECT_INTERVAL =
  30000;

/*
 * Prevent multiple parts of the application
 * from opening multiple Deriv sockets at once.
 */
let derivConnectionPromise = null;

/*
|--------------------------------------------------------------------------
| Request IDs
|--------------------------------------------------------------------------
*/

let requestId = 1;

function nextRequestId() {
  return requestId++;
}

/*
|--------------------------------------------------------------------------
| Active symbols
|--------------------------------------------------------------------------
*/

let activeSymbols = [];
let activeSymbolsLoadedAt = 0;

const ACTIVE_SYMBOL_CACHE_MS =
  10 * 60 * 1000;

let activeSymbolsRequest = null;

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
| Send to Deriv
|--------------------------------------------------------------------------
*/

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

/*
|--------------------------------------------------------------------------
| Send to browser
|--------------------------------------------------------------------------
*/

function sendClient(
  client,
  payload
) {
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
| Broadcast
|--------------------------------------------------------------------------
*/

function broadcast(payload) {
  for (
    const client of browserClients
  ) {
    sendClient(
      client,
      payload
    );
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

    markets:
      activeSymbols.map(
        (market) => ({
          name: market.name,
          symbol: market.symbol,
          pipSize:
            market.pipSize,
          precision:
            precisionFromPipSize(
              market.pipSize
            )
        })
      )
  };
}

function broadcastMarkets() {
  broadcast(
    marketPayload()
  );
}

/*
|--------------------------------------------------------------------------
| Ensure Deriv connection
|--------------------------------------------------------------------------
*/

function ensureDerivConnection() {
  /*
   * Already connected.
   */
  if (
    derivSocket &&
    derivSocket.readyState ===
      WebSocket.OPEN
  ) {
    return Promise.resolve();
  }

  /*
   * Already connecting.
   */
  if (derivConnectionPromise) {
    return derivConnectionPromise;
  }

  /*
   * A reconnect timer is already waiting.
   */
  if (derivReconnectTimer) {
    return Promise.reject(
      new Error(
        "Deriv connection is waiting for rate-limit backoff."
      )
    );
  }

  derivConnectionPromise =
    new Promise(
      (resolve, reject) => {
        const now =
          Date.now();

        const elapsed =
          now -
          derivLastAttempt;

        const wait =
          Math.max(
            0,
            DERIV_MIN_CONNECT_INTERVAL -
              elapsed
          );

        setTimeout(() => {
          connectDeriv(
            resolve,
            reject
          );
        }, wait);
      }
    ).finally(() => {
      derivConnectionPromise =
        null;
    });

  return derivConnectionPromise;
}

/*
|--------------------------------------------------------------------------
| Connect to Deriv
|--------------------------------------------------------------------------
*/

function connectDeriv(
  resolveConnection,
  rejectConnection
) {
  if (derivConnecting) {
    return;
  }

  if (
    derivSocket &&
    (
      derivSocket.readyState ===
        WebSocket.OPEN ||
      derivSocket.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  derivConnecting = true;
  derivLastAttempt =
    Date.now();

  console.log(
    "[DERIV] Connecting to public market WebSocket..."
  );

  const ws =
    new WebSocket(
      DERIV_WS_URL
    );

  derivSocket = ws;

  let settled = false;

  function resolveOnce() {
    if (settled) {
      return;
    }

    settled = true;

    if (
      typeof resolveConnection ===
      "function"
    ) {
      resolveConnection();
    }
  }

  function rejectOnce(err) {
    if (settled) {
      return;
    }

    settled = true;

    if (
      typeof rejectConnection ===
      "function"
    ) {
      rejectConnection(err);
    }
  }

  ws.on("open", async () => {
    derivConnecting = false;

    /*
     * Successful connection.
     *
     * Do not go back to a 2-second
     * reconnect cycle.
     */
    derivBackoff = 30000;

    console.log(
      "[DERIV] Connected."
    );

    try {
      /*
       * Get market list once per connection.
       */
      await loadActiveSymbols(
        true
      );

      console.log(
        `[DERIV] Active Volatility/Jump markets: ${activeSymbols.length}`
      );

      broadcastMarkets();

      /*
       * Restore active browser
       * subscriptions after reconnect.
       */
      resubscribeAll();

      broadcast({
        type:
          "deriv_connected"
      });

      resolveOnce();
    } catch (err) {
      console.error(
        "[DERIV] Initialization error:",
        err.message
      );

      rejectOnce(err);
    }
  });

  ws.on("message", (raw) => {
    handleDerivMessage(
      raw
    );
  });

  ws.on("error", (err) => {
    const message =
      String(
        err?.message || ""
      );

    console.error(
      "[DERIV] WebSocket error:",
      message
    );

    /*
     * HTTP 429 means the Deriv
     * gateway is rate-limiting us.
     */
    if (
      message.includes("429")
    ) {
      derivBackoff =
        Math.max(
          derivBackoff,
          60000
        );

      console.warn(
        "[DERIV] Rate limited (429). Increasing backoff."
      );
    }

    rejectOnce(
      new Error(
        message ||
          "Deriv WebSocket connection failed."
      )
    );
  });

  ws.on("close", (
    code,
    reason
  ) => {
    derivConnecting = false;

    if (
      derivSocket === ws
    ) {
      derivSocket = null;
    }

    const reasonText =
      reason
        ? reason.toString()
        : "";

    console.log(
      `[DERIV] WebSocket closed. code=${code} reason=${reasonText}`
    );

    /*
     * Subscription IDs belong to
     * the previous connection.
     */
    for (
      const subscription of
        symbolSubscriptions.values()
    ) {
      subscription.subscriptionId =
        null;

      subscription.requestId =
        null;
    }

    /*
     * Only reconnect when somebody
     * actually needs market data.
     */
    if (
      browserClients.size > 0 ||
      symbolSubscriptions.size >
        0
    ) {
      broadcast({
        type:
          "deriv_reconnecting"
      });

      scheduleDerivReconnect();
    }
  });
}

/*
|--------------------------------------------------------------------------
| Schedule Deriv reconnect
|--------------------------------------------------------------------------
*/

function scheduleDerivReconnect() {
  if (
    derivReconnectTimer
  ) {
    return;
  }

  /*
   * Nothing needs the connection.
   */
  if (
    browserClients.size === 0 &&
    symbolSubscriptions.size ===
      0
  ) {
    return;
  }

  const jitter =
    Math.floor(
      Math.random() * 10000
    );

  const delay =
    Math.min(
      derivBackoff,
      DERIV_MAX_BACKOFF
    ) + jitter;

  console.log(
    `[DERIV] Reconnecting in ${Math.round(
      delay / 1000
    )}s...`
  );

  derivReconnectTimer =
    setTimeout(() => {
      derivReconnectTimer =
        null;

      if (
        browserClients.size ===
          0 &&
        symbolSubscriptions.size ===
          0
      ) {
        return;
      }

      ensureDerivConnection()
        .catch((err) => {
          console.error(
            "[DERIV] Reconnect failed:",
            err.message
          );

          scheduleDerivReconnect();
        });

      derivBackoff =
        Math.min(
          derivBackoff * 2,
          DERIV_MAX_BACKOFF
        );
    }, delay);
}

/*
|--------------------------------------------------------------------------
| Request Deriv tick subscription
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

  /*
   * Don't create duplicates.
   */
  if (
    subscription.subscriptionId ||
    subscription.requestId
  ) {
    return true;
  }

  const reqId =
    nextRequestId();

  subscription.requestId =
    reqId;

  const sent =
    sendDeriv({
      ticks: symbol,
      subscribe: 1,
      req_id: reqId
    });

  if (!sent) {
    subscription.requestId =
      null;

    return false;
  }

  console.log(
    `[DERIV] Subscribing to ${symbol}`
  );

  return true;
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
    const [
      symbol,
      subscription
    ] of symbolSubscriptions
  ) {
    if (
      subscription.clients.size ===
      0
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
| Handle Deriv messages
|--------------------------------------------------------------------------
*/

function handleDerivMessage(
  raw
) {
  let data;

  try {
    data = JSON.parse(
      raw.toString()
    );
  } catch {
    console.error(
      "[DERIV] Invalid JSON received."
    );

    return;
  }

  /*
  |--------------------------------------------------------------------------
  | Active symbols
  |--------------------------------------------------------------------------
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

    activeSymbols =
      list
        .filter((item) => {
          const name =
            String(
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
          (item) =>
            !item.suspended
        )
        .sort((a, b) =>
          a.name.localeCompare(
            b.name,
            undefined,
            {
              numeric: true,
              sensitivity:
                "base"
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
  |--------------------------------------------------------------------------
  | Tick
  |--------------------------------------------------------------------------
  */

  if (
    data.msg_type === "tick" &&
    data.tick
  ) {
    const tick =
      data.tick;

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
     * Save the Deriv subscription ID.
     */
    if (
      data.subscription &&
      data.subscription.id
    ) {
      subscription.subscriptionId =
        data.subscription.id;

      subscription.requestId =
        null;

      /*
       * User may have stopped while
       * the subscription was being
       * established.
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
          item.symbol ===
          symbol
      );

    const pipSize =
      Number(
        tick.pip_size || 0
      ) ||
      (
        market
          ? Number(
              market.pipSize ||
                0
            )
          : 0
      );

    const payload = {
      type: "tick",

      market:
        market
          ? market.name
          : symbol,

      symbol,

      quote:
        tick.quote,

      epoch:
        tick.epoch,

      pipSize,

      precision:
        market
          ? precisionFromPipSize(
              market.pipSize
            )
          : precisionFromPipSize(
              tick.pip_size
            )
    };

    for (
      const client of
        subscription.clients
    ) {
      sendClient(
        client,
        payload
      );
    }

    return;
  }

  /*
  |--------------------------------------------------------------------------
  | Deriv errors
  |--------------------------------------------------------------------------
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
     * Match failed subscription
     * request.
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

        for (
          const client of
            subscription.clients
        ) {
          sendClient(client, {
            type:
              "deriv_error",

            message:
              data.error
                .message ||
              "Deriv market data error."
          });
        }

        break;
      }
    }

    /*
     * Also handle errors where
     * echo_req.ticks is available.
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
        for (
          const client of
            subscription.clients
        ) {
          sendClient(client, {
            type:
              "deriv_error",

            message:
              data.error
                .message ||
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
| Resolve pending request
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
| Load active symbols
|--------------------------------------------------------------------------
*/

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

  /*
   * Don't send duplicate requests.
   */
  if (activeSymbolsRequest) {
    return activeSymbolsRequest;
  }

  if (
    !derivSocket ||
    derivSocket.readyState !==
      WebSocket.OPEN
  ) {
    return Promise.reject(
      new Error(
        "Deriv WebSocket is not connected."
      )
    );
  }

  activeSymbolsRequest =
    new Promise(
      (resolve, reject) => {
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

            req_id:
              reqId
          });

        if (!sent) {
          clearTimeout(
            timer
          );

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
    Math.min(
      decimals,
      10
    )
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

  /*
   * Exact normalized name.
   *
   * This deliberately keeps:
   *
   * Volatility 75
   *
   * different from:
   *
   * Volatility 75 (1s)
   */
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

  /*
   * Conservative fallback.
   */
  const compact =
    requested
      .replace(
        /[–—]/g,
        "-"
      )
      .replace(
        /\s+/g,
        ""
      );

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
          candidate ===
          compact
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
  /*
   * Ensure there is exactly one
   * shared Deriv connection.
   */
  try {
    await ensureDerivConnection();
  } catch (err) {
    sendClient(client, {
      type:
        "market_error",

      message:
        "Deriv market connection is temporarily unavailable. Please try again shortly."
    });

    return;
  }

  /*
   * Load markets.
   */
  try {
    await loadActiveSymbols(
      false
    );
  } catch (err) {
    sendClient(client, {
      type:
        "market_error",

      message:
        err.message
    });

    return;
  }

  /*
   * Resolve exact market.
   */
  const market =
    resolveExactMarket(
      marketName
    );

  if (!market) {
    sendClient(client, {
      type:
        "market_error",

      message:
        "That market is not currently active on Deriv."
    });

    return;
  }

  /*
   * Remove old browser
   * subscription first.
   */
  unsubscribeClientFromAllMarkets(
    client
  );

  let subscription =
    symbolSubscriptions.get(
      market.symbol
    );

  /*
   * Create one shared subscription.
   */
  if (!subscription) {
    subscription = {
      symbol:
        market.symbol,

      subscriptionId:
        null,

      requestId:
        null,

      clients:
        new Set()
    };

    symbolSubscriptions.set(
      market.symbol,
      subscription
    );

    /*
     * One Deriv tick subscription.
     */
    const subscribed =
      requestDerivSubscription(
        market.symbol,
        subscription
      );

    if (!subscribed) {
      symbolSubscriptions.delete(
        market.symbol
      );

      sendClient(client, {
        type:
          "market_error",

        message:
          "Could not subscribe to the Deriv market."
      });

      return;
    }
  }

  /*
   * Add browser to shared stream.
   */
  subscription.clients.add(
    client
  );

  client.currentSymbol =
    market.symbol;

  client.currentMarket =
    market.name;

  sendClient(client, {
    type:
      "market_connected",

    market:
      market.name,

    symbol:
      market.symbol,

    pipSize:
      market.pipSize,

    precision:
      precisionFromPipSize(
        market.pipSize
      )
  });
}

/*
|--------------------------------------------------------------------------
| Remove browser from subscriptions
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

      /*
       * Nobody is using this symbol.
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
      }
    }
  }

  client.currentSymbol =
    null;

  client.currentMarket =
    null;
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
  if (!subscription) {
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
| WebSocket server
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
       * Only /ws is accepted.
       */
      if (
        url.pathname !== "/ws"
      ) {
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
        wsTokens.get(
          token
        );

      /*
       * One-time token.
       */
      wsTokens.delete(
        token
      );

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
| Browser WebSocket connection
|--------------------------------------------------------------------------
*/

wss.on(
  "connection",
  (client) => {
    /*
     * Extra safety check.
     */
    if (
      !client.authenticated
    ) {
      client.close(
        1008,
        "Unauthorized"
      );

      return;
    }

    browserClients.add(
      client
    );

    client.currentSymbol =
      null;

    client.currentMarket =
      null;

    /*
     * Tell frontend authentication
     * succeeded.
     */
    sendClient(client, {
      type:
        "hello",

      authenticated:
        true
    });

    /*
     * Send cached markets immediately.
     */
    if (
      activeSymbols.length
    ) {
      sendClient(
        client,
        marketPayload()
      );
    }

    /*
     * Browser messages.
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
          data =
            JSON.parse(
              raw.toString()
            );
        } catch {
          sendClient(client, {
            type:
              "error",

            message:
              "Invalid message."
          });

          return;
        }

        /*
         * Request market list.
         */
        if (
          data.action ===
          "markets"
        ) {
          try {
            /*
             * If there is no Deriv
             * connection, create one.
             */
            await ensureDerivConnection();

            await loadActiveSymbols(
              false
            );

            sendClient(
              client,
              marketPayload()
            );
          } catch (err) {
            sendClient(client, {
              type:
                "market_error",

              message:
                err.message
            });
          }

          return;
        }

        /*
         * Start market stream.
         */
        if (
          data.action ===
          "start"
        ) {
          const marketName =
            String(
              data.marketName ||
                ""
            ).trim();

          if (!marketName) {
            sendClient(client, {
              type:
                "market_error",

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
         * Stop market stream.
         */
        if (
          data.action ===
          "stop"
        ) {
          unsubscribeClientFromAllMarkets(
            client
          );

          sendClient(client, {
            type:
              "market_stopped"
          });

          /*
           * If absolutely nobody is
           * using Deriv anymore, close
           * the shared connection.
           */
          shutdownDerivIfUnused();

          return;
        }

        /*
         * Unknown command.
         */
        sendClient(client, {
          type:
            "error",

          message:
            "Unknown WebSocket action."
        });
      }
    );

    /*
     * Browser disconnected.
     */
    client.on(
      "close",
      () => {
        unsubscribeClientFromAllMarkets(
          client
        );

        browserClients.delete(
          client
        );

        shutdownDerivIfUnused();
      }
    );

    /*
     * Browser error.
     */
    client.on(
      "error",
      () => {
        unsubscribeClientFromAllMarkets(
          client
        );

        browserClients.delete(
          client
        );

        shutdownDerivIfUnused();
      }
    );
  }
);

/*
|--------------------------------------------------------------------------
| Shut down unused Deriv connection
|--------------------------------------------------------------------------
*/

function shutdownDerivIfUnused() {
  if (
    browserClients.size > 0 ||
    symbolSubscriptions.size > 0
  ) {
    return;
  }

  if (
    derivReconnectTimer
  ) {
    clearTimeout(
      derivReconnectTimer
    );

    derivReconnectTimer =
      null;
  }

  if (
    derivSocket &&
    (
      derivSocket.readyState ===
        WebSocket.OPEN ||
      derivSocket.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    console.log(
      "[DERIV] No active clients. Closing shared connection."
    );

    try {
      derivSocket.close(
        1000,
        "No active clients"
      );
    } catch {}

    return;
  }

  derivSocket = null;
}

/*
|--------------------------------------------------------------------------
| Pending request safety monitor
|--------------------------------------------------------------------------
*/

setInterval(() => {
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
| Graceful shutdown
|--------------------------------------------------------------------------
*/

function shutdown(signal) {
  console.log(
    `[SERVER] ${signal} received. Shutting down...`
  );

  if (derivSocket) {
    try {
      derivSocket.close(
        1000,
        "Server shutdown"
      );
    } catch {}
  }

  server.close(() => {
    console.log(
      "[SERVER] Shutdown complete."
    );

    process.exit(0);
  });

  setTimeout(() => {
    process.exit(0);
  }, 5000).unref();
}

process.on(
  "SIGINT",
  () => shutdown("SIGINT")
);

process.on(
  "SIGTERM",
  () => shutdown("SIGTERM")
);

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

    console.log(
      "[DERIV] Waiting for a market-data request before connecting."
    );
  }
);
