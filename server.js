require("dotenv").config();

const express = require("express");
const session = require("express-session");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;

const DERIV_APP_ID = process.env.DERIV_APP_ID || "34heQ2dKwBx8bfQnGzodr";

const LOGIN_MARKET = process.env.LOGIN_MARKET || "Market23";
const LOGIN_PASSWORD = process.env.LOGIN_PASSWORD || "Trade23";

const MATCHES_CODE = process.env.MATCHES_CODE || "19809";

const sessionParser = session({
  secret:
    process.env.SESSION_SECRET ||
    "CHANGE_THIS_SECRET_BEFORE_DEPLOYING",
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    secure: false,
    maxAge: 24 * 60 * 60 * 1000
  }
});

app.use(express.json());
app.use(sessionParser);
app.use(express.static("public"));

/* ---------------- LOGIN ---------------- */

app.post("/api/login", (req, res) => {
  const { market, password } = req.body;

  if (
    market === LOGIN_MARKET &&
    password === LOGIN_PASSWORD
  ) {
    req.session.loggedIn = true;
    req.session.matchesUnlocked = false;

    return res.json({
      success: true,
      message: "Login successful"
    });
  }

  return res.status(401).json({
    success: false,
    message: "Wrong Market or Password"
  });
});

/* ---------------- LOGOUT ---------------- */

app.post("/api/logout", (req, res) => {
  req.session.destroy(() => {
    res.json({ success: true });
  });
});

/* ---------------- SESSION ---------------- */

app.get("/api/session", (req, res) => {
  res.json({
    loggedIn: !!req.session.loggedIn,
    matchesUnlocked: !!req.session.matchesUnlocked
  });
});

/* ---------------- MATCHES UNLOCK ---------------- */

app.post("/api/unlock-matches", (req, res) => {
  if (!req.session.loggedIn) {
    return res.status(401).json({
      success: false,
      message: "Please login first"
    });
  }

  const { code } = req.body;

  if (code === MATCHES_CODE) {
    req.session.matchesUnlocked = true;

    return res.json({
      success: true,
      message: "MATCHES unlocked"
    });
  }

  return res.status(403).json({
    success: false,
    message: "Wrong activation code"
  });
});

/* ---------------- DERIV WEBSOCKET ---------------- */

const wss = new WebSocket.Server({
  noServer: true
});

server.on("upgrade", (request, socket, head) => {
  sessionParser(
    request,
    {},
    () => {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit("connection", ws, request);
      });
    }
  );
});

wss.on("connection", (browserWS, request) => {
  if (!request.session || !request.session.loggedIn) {
    browserWS.close();
    return;
  }

  let derivWS = null;

  browserWS.on("message", (message) => {
    try {
      const data = JSON.parse(message.toString());

      /* START MARKET */

      if (data.action === "start") {
        const symbol = data.symbol;

        if (!symbol) {
          return;
        }

        /* Close old Deriv connection */

        if (derivWS) {
          try {
            derivWS.close();
          } catch {}
        }

        /* Connect to Deriv public market data */

        derivWS = new WebSocket(
          "wss://api.derivws.com/trading/v1/options/ws/public"
        );

        derivWS.on("open", () => {
          derivWS.send(
            JSON.stringify({
              active_symbols: "brief"
            })
          );

          derivWS.send(
            JSON.stringify({
              ticks: symbol,
              subscribe: 1
            })
          );

          browserWS.send(
            JSON.stringify({
              type: "status",
              status: "connected",
              symbol
            })
          );
        });

        derivWS.on("message", (raw) => {
          try {
            const tick = JSON.parse(raw.toString());

            if (tick.tick) {
              browserWS.send(
                JSON.stringify({
                  type: "tick",
                  tick: tick.tick
                })
              );
            }
          } catch {}
        });

        derivWS.on("error", () => {
          if (browserWS.readyState === WebSocket.OPEN) {
            browserWS.send(
              JSON.stringify({
                type: "status",
                status: "error"
              })
            );
          }
        });

        derivWS.on("close", () => {
          if (browserWS.readyState === WebSocket.OPEN) {
            browserWS.send(
              JSON.stringify({
                type: "status",
                status: "disconnected"
              })
            );
          }
        });
      }

      /* STOP */

      if (data.action === "stop") {
        if (derivWS) {
          try {
            derivWS.close();
          } catch {}

          derivWS = null;
        }

        if (browserWS.readyState === WebSocket.OPEN) {
          browserWS.send(
            JSON.stringify({
              type: "status",
              status: "stopped"
            })
          );
        }
      }
    } catch {}
  });

  browserWS.on("close", () => {
    if (derivWS) {
      try {
        derivWS.close();
      } catch {}
    }
  });
});

/* ---------------- SERVER ---------------- */

server.listen(PORT, () => {
  console.log(`DERIV LIVE ENTRY running on port ${PORT}`);
});
