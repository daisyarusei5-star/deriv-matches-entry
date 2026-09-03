require("dotenv").config();

const path = require("path");
const http = require("http");
const express = require("express");
const session = require("express-session");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

const PORT = Number(process.env.PORT || 3000);

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const DERIV_APP_ID = process.env.DERIV_APP_ID || "";

const LOGIN_MARKET = process.env.LOGIN_MARKET || "Market23";
const LOGIN_PASSWORD = process.env.LOGIN_PASSWORD || "Trade23";
const MATCHES_CODE = process.env.MATCHES_CODE || "19809";

const SESSION_SECRET =
  process.env.SESSION_SECRET ||
  "CHANGE_THIS_TO_A_LONG_RANDOM_SECRET";

const MAX_RECONNECT_ATTEMPTS = 5;

const MARKETS = [
  {
    id: "vol10_1s",
    name: "Volatility 10 Index, 10 (1s)",
    aliases: ["Volatility 10 (1s) Index", "Volatility 10 Index"],
  },
  {
    id: "vol15_1s",
    name: "Volatility 15 Index, 15 (1s)",
    aliases: ["Volatility 15 (1s) Index", "Volatility 15 Index"],
  },
  {
    id: "vol25_1s",
    name: "Volatility 25 Index, 25 (1s)",
    aliases: ["Volatility 25 (1s) Index", "Volatility 25 Index"],
  },
  {
    id: "vol30_1s",
    name: "Volatility 30 Index, 30 (1s)",
    aliases: ["Volatility 30 (1s) Index", "Volatility 30 Index"],
  },
  {
    id: "vol50_1s",
    name: "Volatility 50 Index, 50 (1s)",
    aliases: ["Volatility 50 (1s) Index", "Volatility 50 Index"],
  },
  {
    id: "vol75_1s",
    name: "Volatility 75 Index, 75 (1s)",
    aliases: ["Volatility 75 (1s) Index", "Volatility 75 Index"],
  },
  {
    id: "vol90_1s",
    name: "Volatility 90 Index, 90 (1s)",
    aliases: ["Volatility 90 (1s) Index", "Volatility 90 Index"],
  },
  {
    id: "vol100_1s",
    name: "Volatility 100 Index, 100 (1s)",
    aliases: ["Volatility 100 (1s) Index", "Volatility 100 Index"],
  },
  {
    id: "vol150_1s",
    name: "Volatility 150 (1s)",
    aliases: ["Volatility 150 (1s) Index", "Volatility 150 Index"],
  },
  {
    id: "vol250_1s",
    name: "Volatility 250 (1s)",
    aliases: ["Volatility 250 (1s) Index", "Volatility 250 Index"],
  },
  {
    id: "jump10",
    name: "Jump 10",
    aliases: ["Jump 10 Index"],
  },
  {
    id: "jump25",
    name: "Jump 25",
    aliases: ["Jump 25 Index"],
  },
  {
    id: "jump50",
    name: "Jump 50",
    aliases: ["Jump 50 Index"],
  },
  {
    id: "jump75",
    name: "Jump 75",
    aliases: ["Jump 75 Index"],
  },
  {
    id: "jump100",
    name: "Jump 100",
    aliases: ["Jump 100 Index"],
  },
];

const CONTRACTS = [
  "ODD",
  "EVEN",
  "OVER",
  "UNDER",
  "DIFFERS",
  "MATCHES",
];

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const sessionMiddleware = session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    maxAge: 24 * 60 * 60 * 1000,
  },
});

app.use(sessionMiddleware);

app.use(express.static(path.join(__dirname, "public")));

function isAuthenticated(req) {
  return Boolean(req.session && req.session.authenticated);
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[,_]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractNumber(text) {
  const match = String(text || "").match(/(\d+)/);
  return match ? Number(match[1]) : null;
}

function hasOneSecond(text) {
  return /\b1\s*s\b|\(1s\)|\b1s\b/i.test(String(text || ""));
}

function isVolatility(text) {
  return /volatility/i.test(String(text || ""));
}

function isJump(text) {
  return /\bjump\b/i.test(String(text || ""));
}

function matchesMarket(uiMarket, active) {
  const candidateName =
    active.underlying_symbol_name ||
    active.display_name ||
    active.name ||
    active.market_display_name ||
    "";

  const candidateSymbol =
    active.underlying_symbol ||
    active.symbol ||
    "";

  const candidate = `${candidateName} ${candidateSymbol}`;
  const wanted = uiMarket.name;

  const wantedNorm = normalizeText(wanted);
  const candidateNorm = normalizeText(candidate);

  if (
    candidateNorm === wantedNorm ||
    candidateNorm.includes(wantedNorm) ||
    wantedNorm.includes(candidateNorm)
  ) {
    return true;
  }

  const wantedNumber = extractNumber(wanted);
  const candidateNumber = extractNumber(candidate);

  if (
    wantedNumber === null ||
    candidateNumber === null ||
    wantedNumber !== candidateNumber
  ) {
    return false;
  }

  const wantedIsVol = isVolatility(wanted);
  const candidateIsVol = isVolatility(candidate);

  const wantedIsJump = isJump(wanted);
  const candidateIsJump = isJump(candidate);

  if (wantedIsVol && !candidateIsVol) {
    return false;
  }

  if (wantedIsJump && !candidateIsJump) {
    return false;
  }

  if (wantedIsVol) {
    return hasOneSecond(wanted) === hasOneSecond(candidate);
  }

  if (wantedIsJump) {
    return true;
  }

  return false;
}

function getActiveSymbolName(active) {
  return (
    active.underlying_symbol_name ||
    active.display_name ||
    active.name ||
    active.underlying_symbol ||
    active.symbol ||
    ""
  );
}

function getActiveSymbol(active) {
  return active.underlying_symbol || active.symbol || "";
}

function getPrecision(active) {
  const raw =
    active.pip_size ??
    active.pipSize ??
    active.decimal_places ??
    active.decimalPlaces ??
    2;

  const n = Number(raw);

  if (!Number.isFinite(n)) {
    return 2;
  }

  if (n >= 0 && n <= 10) {
    return Math.round(n);
  }

  return 2;
}

function sendJson(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }

  try {
    ws.send(JSON.stringify(payload));
  } catch (_) {}
}

function closeQuietly(ws) {
  try {
    if (ws && ws.readyState !== WebSocket.CLOSED) {
      ws.close();
    }
  } catch (_) {}
}

/* ---------------- API ---------------- */

app.get("/api/config", (req, res) => {
  res.json({
    markets: MARKETS.map((m) => ({
      id: m.id,
      name: m.name,
    })),
    contracts: CONTRACTS,
    appIdConfigured: Boolean(DERIV_APP_ID),
  });
});

app.get("/api/session", (req, res) => {
  res.json({
    authenticated: isAuthenticated(req),
    matchesUnlocked: Boolean(req.session?.matchesUnlocked),
  });
});

app.post("/api/login", (req, res) => {
  const market = String(req.body.market || "").trim();
  const password = String(req.body.password || "");

  if (
    market === LOGIN_MARKET &&
    password === LOGIN_PASSWORD
  ) {
    req.session.authenticated = true;

    return req.session.save(() => {
      res.json({
        ok: true,
        matchesUnlocked: Boolean(req.session.matchesUnlocked),
      });
    });
  }

  return res.status(401).json({
    ok: false,
    message: "INVALID LOGIN DETAILS",
  });
});

app.post("/api/logout", (req, res) => {
  req.session.destroy(() => {
    res.json({ ok: true });
  });
});

app.post("/api/unlock-matches", (req, res) => {
  if (!isAuthenticated(req)) {
    return res.status(401).json({
      ok: false,
      message: "LOGIN REQUIRED",
    });
  }

  const code = String(req.body.code || "").trim();

  if (code !== MATCHES_CODE) {
    return res.status(403).json({
      ok: false,
      message: "INVALID MATCHES CODE",
    });
  }

  req.session.matchesUnlocked = true;

  return req.session.save(() => {
    res.json({
      ok: true,
      message: "MATCHES UNLOCKED",
    });
  });
});

/* ---------------- WebSocket session protection ---------------- */

server.on("upgrade", (request, socket, head) => {
  if (!request.url || !request.url.startsWith("/live")) {
    socket.destroy();
    return;
  }

  const fakeResponse = {
    getHeader() {
      return undefined;
    },

    setHeader() {},

    removeHeader() {},

    writeHead() {},

    end() {
      try {
        socket.destroy();
      } catch (_) {}
    },
  };

  sessionMiddleware(request, fakeResponse, () => {
    if (!request.session?.authenticated) {
      try {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n" +
            "Connection: close\r\n" +
            "\r\n"
        );
      } catch (_) {}

      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit("connection", ws, request);
    });
  });
});

/* ---------------- Live Deriv connection ---------------- */

wss.on("connection", (clientWs, request) => {
  let derivWs = null;

  let generation = 0;
  let reconnectAttempt = 0;
  let reconnectTimer = null;
  let intentionallyStopped = false;

  let selectedMarket = null;
  let selectedContract = null;

  let selectedSymbol = null;
  let selectedPrecision = 2;
  let actualMarketName = null;

  let marketReady = false;

  /*
   * MATCHES status comes from the authenticated HTTP session.
   */
  const matchesUnlocked = Boolean(
    request.session?.matchesUnlocked
  );

  clientWs._matchesUnlocked = matchesUnlocked;

  function clientSend(payload) {
    sendJson(clientWs, payload);
  }

  function clearReconnectTimer() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function destroyDerivSocket() {
    if (derivWs) {
      const old = derivWs;
      derivWs = null;

      try {
        old.removeAllListeners();
      } catch (_) {}

      closeQuietly(old);
    }
  }

  function getReconnectDelay(attempt) {
    const delays = [
      350,
      700,
      1200,
      2000,
      3000,
    ];

    return delays[
      Math.min(attempt - 1, delays.length - 1)
    ];
  }

  function scheduleReconnect(reason) {
    if (intentionallyStopped || !selectedMarket) {
      return;
    }

    if (reconnectAttempt >= MAX_RECONNECT_ATTEMPTS) {
      clientSend({
        type: "connection",
        state: "failed",
        message:
          "DERIV CONNECTION FAILED — DON'T TRADE",
        reason: reason || "connection closed",
      });

      return;
    }

    reconnectAttempt += 1;

    const delay = getReconnectDelay(reconnectAttempt);

    clientSend({
      type: "connection",
      state: "reconnecting",
      attempt: reconnectAttempt,
      maxAttempts: MAX_RECONNECT_ATTEMPTS,
      delay,
      message:
        `RECONNECTING ${reconnectAttempt}/` +
        `${MAX_RECONNECT_ATTEMPTS}...`,
    });

    clearReconnectTimer();

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connectToDeriv();
    }, delay);
  }

  function connectToDeriv() {
    if (
      intentionallyStopped ||
      !selectedMarket
    ) {
      return;
    }

    const myGeneration = generation;

    destroyDerivSocket();

    marketReady = false;
    selectedSymbol = null;
    actualMarketName = null;

    clientSend({
      type: "connection",
      state: "connecting",
      message: "CONNECTING TO DERIV...",
    });

    try {
      derivWs = new WebSocket(
        DERIV_WS_URL,
        {
          handshakeTimeout: 7000,
        }
      );
    } catch (err) {
      scheduleReconnect(err?.message);
      return;
    }

    const currentSocket = derivWs;

    currentSocket.on("open", () => {
      if (
        intentionallyStopped ||
        myGeneration !== generation ||
        currentSocket !== derivWs
      ) {
        return;
      }

      reconnectAttempt = 0;

      clientSend({
        type: "connection",
        state: "connected",
        message:
          "DERIV CONNECTED — FINDING MARKET",
      });

      sendJson(currentSocket, {
        active_symbols: "brief",
        req_id: 1001,
      });
    });

    currentSocket.on("message", (raw) => {
      if (
        intentionallyStopped ||
        myGeneration !== generation ||
        currentSocket !== derivWs
      ) {
        return;
      }

      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch (_) {
        return;
      }

      if (data.error) {
        clientSend({
          type: "deriv_error",
          message:
            data.error.message ||
            data.error.code ||
            "DERIV ERROR",
        });

        if (!marketReady) {
          scheduleReconnect(
            data.error.message ||
              "Deriv API error"
          );
        }

        return;
      }

      /* ACTIVE SYMBOLS */

      if (data.msg_type === "active_symbols") {
        const list = Array.isArray(
          data.active_symbols
        )
          ? data.active_symbols
          : [];

        const match = list.find((item) =>
          matchesMarket(
            selectedMarket,
            item
          )
        );

        if (!match) {
          clientSend({
            type: "connection",
            state: "market_not_found",
            message:
              "SELECTED MARKET IS NOT CURRENTLY AVAILABLE",
          });

          scheduleReconnect(
            "market not found"
          );

          return;
        }

        const symbol = getActiveSymbol(match);

        if (!symbol) {
          clientSend({
            type: "connection",
            state: "failed",
            message:
              "DERIV MARKET SYMBOL NOT FOUND — DON'T TRADE",
          });

          scheduleReconnect(
            "empty symbol"
          );

          return;
        }

        selectedSymbol = symbol;
        selectedPrecision = getPrecision(match);
        actualMarketName =
          getActiveSymbolName(match);

        marketReady = true;

        clientSend({
          type: "market_ready",
          marketName: selectedMarket.name,
          actualMarketName,
          symbol: selectedSymbol,
          precision: selectedPrecision,
          contract: selectedContract,
        });

        clientSend({
          type: "connection",
          state: "live",
          message:
            "LIVE MARKET CONNECTED",
        });

        /* Load recent exact-market history */

        sendJson(currentSocket, {
          ticks_history: selectedSymbol,
          count: 60,
          end: "latest",
          style: "ticks",
          req_id: 2001,
        });

        /* Subscribe to SAME exact symbol */

        sendJson(currentSocket, {
          ticks: selectedSymbol,
          subscribe: 1,
          req_id: 2002,
        });

        return;
      }

      /* TICKS HISTORY */

      if (
        data.msg_type === "history" ||
        data.msg_type === "ticks_history"
      ) {
        const prices = Array.isArray(
          data.history?.prices
        )
          ? data.history.prices
          : [];

        const times = Array.isArray(
          data.history?.times
        )
          ? data.history.times
          : [];

        const history = prices
          .map((price, index) => ({
            quote: Number(price),
            epoch: Number(
              times[index] ||
                Date.now() / 1000
            ),
            symbol: selectedSymbol,
          }))
          .filter((item) =>
            Number.isFinite(item.quote)
          );

        if (history.length > 0) {
          clientSend({
            type: "history",
            marketName:
              selectedMarket.name,
            actualMarketName,
            symbol: selectedSymbol,
            precision:
              selectedPrecision,
            ticks: history,
          });
        }

        return;
      }

      /* LIVE TICK */

      if (
        data.msg_type === "tick" &&
        data.tick
      ) {
        const tick = data.tick;

        if (
          !selectedSymbol ||
          String(tick.symbol || "") !==
            String(selectedSymbol)
        ) {
          return;
        }

        const quote = Number(tick.quote);

        if (!Number.isFinite(quote)) {
          return;
        }

        clientSend({
          type: "tick",
          marketName:
            selectedMarket.name,
          actualMarketName,
          symbol: selectedSymbol,
          precision:
            selectedPrecision,
          tick: {
            quote,
            epoch: Number(
              tick.epoch ||
                Date.now() / 1000
            ),
            symbol: tick.symbol,
          },
        });
      }
    });

    currentSocket.on("error", (err) => {
      if (
        intentionallyStopped ||
        myGeneration !== generation ||
        currentSocket !== derivWs
      ) {
        return;
      }

      clientSend({
        type: "connection",
        state: "temporary_error",
        message:
          "DERIV CONNECTION ISSUE — RETRYING...",
        detail: err?.message || "",
      });
    });

    currentSocket.on("close", () => {
      if (
        intentionallyStopped ||
        myGeneration !== generation ||
        currentSocket !== derivWs
      ) {
        return;
      }

      derivWs = null;
      marketReady = false;

      scheduleReconnect(
        "socket closed"
      );
    });
  }

  function startMarket(
    marketId,
    contract
  ) {
    const market = MARKETS.find(
      (item) =>
        item.id === marketId
    );

    if (!market) {
      clientSend({
        type: "connection",
        state: "failed",
        message: "INVALID MARKET",
      });

      return;
    }

    if (!CONTRACTS.includes(contract)) {
      clientSend({
        type: "connection",
        state: "failed",
        message: "INVALID CONTRACT",
      });

      return;
    }

    if (
      contract === "MATCHES" &&
      !clientWs._matchesUnlocked
    ) {
      clientSend({
        type: "connection",
        state: "failed",
        message:
          "MATCHES IS LOCKED",
      });

      return;
    }

    generation += 1;

    clearReconnectTimer();
    destroyDerivSocket();

    intentionallyStopped = false;

    selectedMarket = market;
    selectedContract = contract;

    selectedSymbol = null;
    actualMarketName = null;
    marketReady = false;

    reconnectAttempt = 0;

    clientSend({
      type: "reset",
      marketName: market.name,
      contract,
    });

    connectToDeriv();
  }

  function stopMarket() {
    generation += 1;

    intentionallyStopped = true;

    clearReconnectTimer();
    destroyDerivSocket();

    selectedSymbol = null;
    actualMarketName = null;
    marketReady = false;

    clientSend({
      type: "connection",
      state: "stopped",
      message:
        "STOPPED — DON'T TRADE",
    });
  }

  clientWs.on("message", (raw) => {
    let message;

    try {
      message = JSON.parse(
        raw.toString()
      );
    } catch (_) {
      return;
    }

    if (
      !message ||
      typeof message !== "object"
    ) {
      return;
    }

    if (message.type === "start") {
      startMarket(
        String(
          message.marketId || ""
        ),
        String(
          message.contract || ""
        )
      );

      return;
    }

    if (message.type === "stop") {
      stopMarket();
      return;
    }

    if (
      message.type ===
      "matches_status"
    ) {
      clientWs._matchesUnlocked =
        Boolean(message.unlocked);
    }
  });

  clientWs.on("close", () => {
    intentionallyStopped = true;

    clearReconnectTimer();
    destroyDerivSocket();
  });

  clientWs.on("error", () => {
    intentionallyStopped = true;

    clearReconnectTimer();
    destroyDerivSocket();
  });

  clientSend({
    type: "ready",
    message: "READY",
  });
});

/*
 * Express 5-safe fallback.
 *
 * Do NOT use app.get("*") here because Express 5
 * can reject the old wildcard syntax.
 */
app.use((req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

server.listen(PORT, () => {
  console.log(
    `DERIV LIVE ENTRY running on port ${PORT}`
  );

  console.log(
    `Deriv public WebSocket: ${DERIV_WS_URL}`
  );
});
