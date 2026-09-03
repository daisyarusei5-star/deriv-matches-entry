require("dotenv").config();

const express = require("express");
const session = require("express-session");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

const PORT = process.env.PORT || 3000;

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const SESSION_SECRET =
  process.env.SESSION_SECRET ||
  "ELISY254_CHANGE_THIS";

const LOGIN_MARKET =
  process.env.LOGIN_MARKET || "Market23";

const LOGIN_PASSWORD =
  process.env.LOGIN_PASSWORD || "Trade23";

const MATCHES_CODE =
  process.env.MATCHES_CODE || "19809";

/* ---------------- SESSION ---------------- */

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

app.use(express.json());
app.use(sessionMiddleware);
app.use(express.static("public"));

const sessionStore = sessionMiddleware.store;

/* ---------------- HELPERS ---------------- */

function normalize(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[–—]/g, "-")
    .replace(/\s+/g, " ")
    .trim();
}

function getName(item) {
  return (
    item.underlying_symbol_name ||
    item.display_name ||
    item.symbol_name ||
    item.name ||
    item.symbol ||
    ""
  );
}

function getSymbol(item) {
  return (
    item.underlying_symbol ||
    item.symbol ||
    item.symbol_name ||
    ""
  );
}

function isWantedMarket(item) {
  const text = getName(item).toLowerCase();

  return (
    text.includes("volatility") ||
    text.includes("jump")
  );
}

function precisionFromPipSize(pipSize) {
  const pip = Number(pipSize);

  if (!Number.isFinite(pip) || pip <= 0 || pip >= 1) {
    return null;
  }

  return Math.max(
    0,
    Math.round(-Math.log10(pip))
  );
}

function discoveredMarkets(items) {
  const result = [];
  const seen = new Set();

  for (const item of items) {
    if (!isWantedMarket(item)) continue;

    const symbol = getSymbol(item);
    const name = getName(item);

    if (!symbol || !name) continue;

    const key = `${symbol}::${normalize(name)}`;

    if (seen.has(key)) continue;

    seen.add(key);

    result.push({
      symbol,
      name,
      precision: precisionFromPipSize(
        item.pip_size
      ),
      pipSize:
        item.pip_size !== undefined
          ? Number(item.pip_size)
          : null,
    });
  }

  result.sort((a, b) =>
    a.name.localeCompare(b.name, undefined, {
      numeric: true,
      sensitivity: "base",
    })
  );

  return result;
}

/* ---------------- LOGIN ---------------- */

app.post("/api/login", (req, res) => {
  const { market, password } = req.body || {};

  if (
    String(market || "") !== LOGIN_MARKET ||
    String(password || "") !== LOGIN_PASSWORD
  ) {
    return res.status(401).json({
      ok: false,
      error: "Wrong market or password.",
    });
  }

  req.session.authenticated = true;

  res.json({ ok: true });
});

app.get("/api/session", (req, res) => {
  res.json({
    authenticated: !!req.session.authenticated,
    matchesUnlocked:
      !!req.session.matchesUnlocked,
  });
});

app.post("/api/logout", (req, res) => {
  req.session.destroy(() => {
    res.json({ ok: true });
  });
});

/* ---------------- MATCHES ---------------- */

app.post("/api/unlock-matches", (req, res) => {
  if (!req.session.authenticated) {
    return res.status(401).json({
      ok: false,
      error: "Not authenticated.",
    });
  }

  const { code } = req.body || {};

  if (String(code || "") !== MATCHES_CODE) {
    return res.status(403).json({
      ok: false,
      error: "Invalid activation code.",
    });
  }

  req.session.matchesUnlocked = true;

  res.json({
    ok: true,
    unlocked: true,
  });
});

/* ---------------- WEBSOCKET SESSION ---------------- */

server.on("upgrade", (request, socket, head) => {
  if (!request.url.startsWith("/live")) {
    socket.destroy();
    return;
  }

  const originalUrl = request.url;
  const cookieHeader =
    request.headers.cookie || "";

  const fakeResponse = {
    getHeader() {
      return undefined;
    },
    setHeader() {},
    writeHead() {},
  };

  const fakeRequest = {
    headers: request.headers,
    connection: request.connection,
    url: originalUrl,
  };

  sessionMiddleware(
    fakeRequest,
    fakeResponse,
    () => {
      const sessionData =
        fakeRequest.session;

      if (
        !sessionData ||
        !sessionData.authenticated
      ) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n" +
            "Connection: close\r\n\r\n"
        );

        socket.destroy();
        return;
      }

      wss.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {
          ws.sessionData = sessionData;

          wss.emit(
            "connection",
            ws,
            request
          );
        }
      );
    }
  );
});

/* ---------------- DERIV CONNECTION ---------------- */

wss.on("connection", (client) => {
  let deriv = null;
  let selectedMarket = null;
  let requestId = 1;

  function send(payload) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(payload));
    }
  }

  function closeDeriv() {
    selectedMarket = null;

    if (deriv) {
      try {
        deriv.close();
      } catch (_) {}

      deriv = null;
    }
  }

  function startMarket(marketName) {
    closeDeriv();

    selectedMarket = marketName;

    deriv = new WebSocket(DERIV_WS_URL);

    deriv.on("open", () => {
      send({
        type: "deriv_status",
        status: "connected",
        marketName,
      });

      /*
        Dynamic Deriv market discovery.
        No hard-coded market list.
      */
      deriv.send(
        JSON.stringify({
          active_symbols: "brief",
          req_id: requestId++,
        })
      );
    });

    deriv.on("message", (raw) => {
      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch (_) {
        return;
      }

      /* ---------- MARKET DISCOVERY ---------- */

      if (data.msg_type === "active_symbols") {
        const markets =
          discoveredMarkets(
            Array.isArray(data.active_symbols)
              ? data.active_symbols
              : []
          );

        send({
          type: "markets",
          markets,
        });

        /*
          Exact matching.

          We keep "(1s)" in the name so:
          Volatility 10 Index
          and
          Volatility 10 Index (1s)

          can never be mixed.
        */
        const selected = markets.find(
          (m) =>
            normalize(m.name) ===
            normalize(marketName)
        );

        if (!selected) {
          send({
            type: "deriv_status",
            status: "market_not_found",
            marketName,
          });

          closeDeriv();
          return;
        }

        send({
          type: "market_confirmed",
          market: selected,
        });

        /* ---------- LIVE TICKS ---------- */

        deriv.send(
          JSON.stringify({
            ticks: selected.symbol,
            subscribe: 1,
            req_id: requestId++,
          })
        );

        /* ---------- REAL HISTORY ---------- */

        deriv.send(
          JSON.stringify({
            ticks_history: selected.symbol,
            count: 60,
            end: "latest",
            style: "ticks",
            req_id: requestId++,
          })
        );

        return;
      }

      /* ---------- HISTORY ---------- */

      if (data.msg_type === "history") {
        send({
          type: "history",
          history: data.history || [],
          times: data.times || [],
        });

        return;
      }

      /* ---------- LIVE TICK ---------- */

      if (
        data.msg_type === "tick" &&
        data.tick
      ) {
        if (!selectedMarket) return;

        send({
          type: "tick",
          marketName: selectedMarket,
          tick: {
            symbol: data.tick.symbol,
            quote: data.tick.quote,
            epoch: data.tick.epoch,
            pip_size:
              data.tick.pip_size,
          },
        });

        return;
      }

      /* ---------- ERRORS ---------- */

      if (data.error) {
        send({
          type: "deriv_error",
          error:
            data.error.message ||
            "Deriv returned an error.",
        });
      }
    });

    deriv.on("error", (error) => {
      send({
        type: "deriv_status",
        status: "error",
        marketName,
        message:
          error?.message ||
          "Deriv connection error.",
      });
    });

    deriv.on("close", () => {
      if (selectedMarket === marketName) {
        send({
          type: "deriv_status",
          status: "closed",
          marketName,
        });
      }
    });
  }

  client.on("message", (raw) => {
    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }

    if (message.action === "start") {
      const marketName = String(
        message.marketName || ""
      ).trim();

      if (!marketName) {
        send({
          type: "deriv_status",
          status: "error",
          message: "Select a market first.",
        });

        return;
      }

      startMarket(marketName);
      return;
    }

    if (message.action === "stop") {
      closeDeriv();

      send({
        type: "deriv_status",
        status: "stopped",
      });
    }
  });

  client.on("close", () => {
    closeDeriv();
  });
});

/* ---------------- START ---------------- */

server.listen(PORT, () => {
  console.log(
    `DERIV LIVE ENTRY listening on port ${PORT}`
  );
});
