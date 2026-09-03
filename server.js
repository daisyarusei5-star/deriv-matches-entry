require("dotenv").config();

const express = require("express");
const session = require("express-session");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;

const SESSION_SECRET =
  process.env.SESSION_SECRET || "CHANGE_THIS_SECRET";

const LOGIN_MARKET =
  process.env.LOGIN_MARKET || "Market23";

const LOGIN_PASSWORD =
  process.env.LOGIN_PASSWORD || "Trade23";

const MATCHES_CODE =
  process.env.MATCHES_CODE || "19809";

const DERIV_APP_ID =
  process.env.DERIV_APP_ID || "";

const DERIV_PUBLIC_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/* -------------------------------------------------------
   SESSION
------------------------------------------------------- */

const sessionMiddleware = session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    maxAge: 1000 * 60 * 60 * 8,
  },
});

app.use(express.json());
app.use(sessionMiddleware);
app.use(express.static("public"));

/* -------------------------------------------------------
   LOGIN
------------------------------------------------------- */

app.post("/api/login", (req, res) => {
  const { market, password } = req.body || {};

  if (
    String(market || "") !== LOGIN_MARKET ||
    String(password || "") !== LOGIN_PASSWORD
  ) {
    return res.status(401).json({
      ok: false,
      message: "Wrong Market or Password.",
    });
  }

  req.session.authenticated = true;
  req.session.matchesUnlocked = false;

  res.json({
    ok: true,
    message: "Login successful.",
  });
});

app.get("/api/session", (req, res) => {
  res.json({
    authenticated: !!req.session.authenticated,
    matchesUnlocked: !!req.session.matchesUnlocked,
  });
});

app.post("/api/logout", (req, res) => {
  req.session.destroy(() => {
    res.json({ ok: true });
  });
});

/* -------------------------------------------------------
   MATCHES
------------------------------------------------------- */

app.post("/api/unlock-matches", (req, res) => {
  if (!req.session.authenticated) {
    return res.status(401).json({
      ok: false,
      message: "Please login first.",
    });
  }

  const { code } = req.body || {};

  if (String(code || "") !== MATCHES_CODE) {
    return res.status(403).json({
      ok: false,
      message: "Invalid MATCHES activation code.",
    });
  }

  req.session.matchesUnlocked = true;

  res.json({
    ok: true,
    message: "MATCHES unlocked.",
  });
});

/* -------------------------------------------------------
   MARKET HELPERS
------------------------------------------------------- */

function normalizeMarketName(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[–—]/g, "-")
    .replace(/\s+/g, " ");
}

function symbolName(item) {
  return (
    item.underlying_symbol_name ||
    item.display_name ||
    item.market_display_name ||
    item.name ||
    ""
  );
}

function symbolCode(item) {
  return (
    item.underlying_symbol ||
    item.symbol ||
    ""
  );
}

function getPrecision(item) {
  const pipSize = Number(item.pip_size);

  if (!Number.isFinite(pipSize) || pipSize <= 0) {
    return null;
  }

  const decimals = Math.max(
    0,
    Math.round(-Math.log10(pipSize))
  );

  return {
    pipSize,
    decimals,
  };
}

function resolveMarket(activeSymbols, requestedName) {
  const wanted = normalizeMarketName(requestedName);

  /*
    Exact name match.
    "(1s)" remains part of the name.
  */
  const exact = activeSymbols.find(
    (item) =>
      normalizeMarketName(symbolName(item)) === wanted
  );

  if (exact) {
    return exact;
  }

  /*
    Fallback only removes brackets/punctuation.
    It does NOT remove "1s".
  */
  const relaxedWanted = wanted
    .replace(/[()]/g, "")
    .replace(/\s+/g, " ")
    .trim();

  return (
    activeSymbols.find((item) => {
      const name = normalizeMarketName(
        symbolName(item)
      )
        .replace(/[()]/g, "")
        .replace(/\s+/g, " ")
        .trim();

      return name === relaxedWanted;
    }) || null
  );
}

/* -------------------------------------------------------
   DERIV CONNECTION
------------------------------------------------------- */

class DerivConnection {
  constructor(browserSocket) {
    this.browserSocket = browserSocket;
    this.derivSocket = null;

    this.marketName = "";
    this.symbol = "";
    this.precision = null;

    this.requestId = 0;
    this.generation = 0;
  }

  send(data) {
    if (
      this.browserSocket.readyState ===
      WebSocket.OPEN
    ) {
      this.browserSocket.send(
        JSON.stringify(data)
      );
    }
  }

  stop() {
    this.generation++;

    if (this.derivSocket) {
      try {
        this.derivSocket.close();
      } catch (_) {}

      this.derivSocket = null;
    }

    this.marketName = "";
    this.symbol = "";
    this.precision = null;
  }

  start(marketName) {
    this.stop();

    this.marketName = marketName;

    const generation = this.generation;

    this.send({
      type: "market-status",
      status: "connecting",
      marketName,
      message:
        `Connecting to ${marketName}...`,
    });

    const ws = new WebSocket(
      DERIV_PUBLIC_WS
    );

    this.derivSocket = ws;

    ws.on("open", () => {
      if (generation !== this.generation) {
        ws.close();
        return;
      }

      this.send({
        type: "market-status",
        status: "connected",
        marketName,
        message:
          "Connected to Deriv live market feed.",
      });

      /*
        Resolve the user's selected market from
        Deriv's currently active symbols.
      */
      ws.send(
        JSON.stringify({
          active_symbols: "brief",
          req_id: ++this.requestId,
        })
      );
    });

    ws.on("message", (raw) => {
      if (generation !== this.generation) {
        return;
      }

      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch (_) {
        return;
      }

      if (data.error) {
        this.send({
          type: "market-error",
          message:
            data.error.message ||
            "Deriv returned an error.",
        });

        return;
      }

      /* ---------------------------------------------
         ACTIVE SYMBOLS
      --------------------------------------------- */

      if (
        data.msg_type === "active_symbols"
      ) {
        const symbols =
          Array.isArray(data.active_symbols)
            ? data.active_symbols
            : [];

        const selected = resolveMarket(
          symbols,
          this.marketName
        );

        if (!selected) {
          this.send({
            type: "market-error",
            message:
              `${this.marketName} is not currently active on Deriv.`,
          });

          this.stop();
          return;
        }

        const exactSymbol =
          symbolCode(selected);

        if (!exactSymbol) {
          this.send({
            type: "market-error",
            message:
              `No Deriv symbol was returned for ${this.marketName}.`,
          });

          this.stop();
          return;
        }

        this.symbol = exactSymbol;
        this.precision =
          getPrecision(selected);

        this.send({
          type: "market-resolved",
          marketName: this.marketName,
          symbol: this.symbol,
          precision: this.precision,
          message:
            `LIVE • ${this.marketName}`,
        });

        /*
          Subscribe to ONLY the selected symbol.
        */
        ws.send(
          JSON.stringify({
            ticks: this.symbol,
            subscribe: 1,
            req_id: ++this.requestId,
          })
        );

        return;
      }

      /* ---------------------------------------------
         LIVE TICK
      --------------------------------------------- */

      if (
        data.msg_type === "tick" &&
        data.tick
      ) {
        const tick = data.tick;

        /*
          Critical market-isolation check.
        */
        if (
          !this.symbol ||
          String(tick.symbol) !==
            String(this.symbol)
        ) {
          return;
        }

        let precision = this.precision;

        if (
          !precision &&
          tick.pip_size
        ) {
          precision = getPrecision({
            pip_size: tick.pip_size,
          });
        }

        this.send({
          type: "tick",

          /*
            These fields let the browser verify that
            the tick belongs to the selected market.
          */
          marketName: this.marketName,
          symbol: this.symbol,

          quote: tick.quote,
          epoch: tick.epoch,

          precision,
        });
      }
    });

    ws.on("error", (error) => {
      if (generation !== this.generation) {
        return;
      }

      this.send({
        type: "market-error",
        message:
          error?.message ||
          "Deriv WebSocket error.",
      });
    });

    ws.on("close", () => {
      if (generation !== this.generation) {
        return;
      }

      this.send({
        type: "market-status",
        status: "disconnected",
        marketName: this.marketName,
        message:
          "Deriv live feed disconnected.",
      });
    });
  }
}

/* -------------------------------------------------------
   BROWSER LIVE SOCKET
------------------------------------------------------- */

const wss = new WebSocket.Server({
  noServer: true,
});

server.on("upgrade", (request, socket, head) => {
  const pathname = new URL(
    request.url,
    `http://${request.headers.host}`
  ).pathname;

  if (pathname !== "/live") {
    socket.destroy();
    return;
  }

  sessionMiddleware(
    request,
    {},
    () => {
      /*
        Public Deriv market data itself does not require
        authentication, but our application does.
      */

      wss.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {
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

wss.on("connection", (browserSocket, request) => {
  const connection =
    new DerivConnection(browserSocket);

  browserSocket.send(
    JSON.stringify({
      type: "ready",
      derivFeed: DERIV_PUBLIC_WS,
      appIdConfigured:
        Boolean(DERIV_APP_ID),
    })
  );

  browserSocket.on("message", (raw) => {
    let message;

    try {
      message = JSON.parse(
        raw.toString()
      );
    } catch (_) {
      return;
    }

    if (
      message.action === "start"
    ) {
      const marketName =
        String(
          message.marketName || ""
        ).trim();

      if (!marketName) {
        connection.send({
          type: "market-error",
          message:
            "Please select a market.",
        });

        return;
      }

      connection.start(
        marketName
      );
    }

    if (
      message.action === "stop"
    ) {
      connection.stop();

      connection.send({
        type: "market-status",
        status: "stopped",
        message:
          "Market stream stopped.",
      });
    }
  });

  browserSocket.on("close", () => {
    connection.stop();
  });

  browserSocket.on("error", () => {
    connection.stop();
  });
});

/* -------------------------------------------------------
   HEALTH
------------------------------------------------------- */

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    service: "DERIV LIVE ENTRY",
    feed: "Deriv public WebSocket",
  });
});

/* -------------------------------------------------------
   START
------------------------------------------------------- */

server.listen(PORT, () => {
  console.log(
    `DERIV LIVE ENTRY running on port ${PORT}`
  );

  console.log(
    `Deriv feed: ${DERIV_PUBLIC_WS}`
  );
});
