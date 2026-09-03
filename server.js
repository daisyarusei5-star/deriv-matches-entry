require("dotenv").config();

const express = require("express");
const session = require("express-session");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
  noServer: true,
});

const PORT = Number(process.env.PORT) || 3000;

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const SESSION_SECRET =
  process.env.SESSION_SECRET ||
  "ELISY254_CHANGE_THIS";

const LOGIN_MARKET =
  process.env.LOGIN_MARKET ||
  "Market23";

const LOGIN_PASSWORD =
  process.env.LOGIN_PASSWORD ||
  "Trade23";

const MATCHES_CODE =
  process.env.MATCHES_CODE ||
  "19809";

/* =====================================================
   EXPRESS
===================================================== */

app.set("trust proxy", 1);

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

    maxAge:
      24 * 60 * 60 * 1000,
  },
});

app.use(sessionMiddleware);

app.use(
  express.static("public")
);

/* =====================================================
   HELPERS
===================================================== */

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

function getPipSize(item) {
  if (
    item.pip_size !== undefined &&
    item.pip_size !== null
  ) {
    const value = Number(item.pip_size);

    if (Number.isFinite(value)) {
      return value;
    }
  }

  if (
    item.pip !== undefined &&
    item.pip !== null
  ) {
    const value = Number(item.pip);

    if (Number.isFinite(value)) {
      return value;
    }
  }

  return null;
}

function precisionFromPipSize(pipSize) {
  const pip = Number(pipSize);

  if (
    !Number.isFinite(pip) ||
    pip <= 0 ||
    pip >= 1
  ) {
    return null;
  }

  return Math.max(
    0,
    Math.round(-Math.log10(pip))
  );
}

function isWantedMarket(item) {
  const name =
    getName(item).toLowerCase();

  return (
    name.includes("volatility") ||
    name.includes("jump")
  );
}

function isTradable(item) {
  /*
    Different Deriv responses can expose
    different trading-status fields.

    If the field is absent, we allow it.
  */

  if (
    item.is_trading_suspended !==
      undefined &&
    Number(
      item.is_trading_suspended
    ) === 1
  ) {
    return false;
  }

  if (
    item.exchange_is_open !==
      undefined &&
    Number(
      item.exchange_is_open
    ) === 0
  ) {
    return false;
  }

  return true;
}

function discoveredMarkets(items) {
  const markets = [];
  const seen = new Set();

  for (const item of items) {
    if (!isWantedMarket(item)) {
      continue;
    }

    if (!isTradable(item)) {
      continue;
    }

    const symbol =
      getSymbol(item);

    const name =
      getName(item);

    if (!symbol || !name) {
      continue;
    }

    const key =
      `${symbol}::${normalize(name)}`;

    if (seen.has(key)) {
      continue;
    }

    seen.add(key);

    const pipSize =
      getPipSize(item);

    markets.push({
      symbol,
      name,

      precision:
        precisionFromPipSize(
          pipSize
        ),

      pipSize,
    });
  }

  markets.sort(
    (a, b) =>
      a.name.localeCompare(
        b.name,
        undefined,
        {
          numeric: true,
          sensitivity: "base",
        }
      )
  );

  return markets;
}

/* =====================================================
   LOGIN
===================================================== */

app.post(
  "/api/login",
  (req, res) => {
    const {
      market,
      password,
    } = req.body || {};

    if (
      String(market || "") !==
        LOGIN_MARKET ||
      String(password || "") !==
        LOGIN_PASSWORD
    ) {
      return res
        .status(401)
        .json({
          ok: false,
          error:
            "Wrong market or password.",
        });
    }

    req.session.authenticated =
      true;

    req.session.matchesUnlocked =
      false;

    res.json({
      ok: true,
    });
  }
);

/* =====================================================
   SESSION
===================================================== */

app.get(
  "/api/session",
  (req, res) => {
    res.json({
      authenticated:
        !!req.session.authenticated,

      matchesUnlocked:
        !!req.session.matchesUnlocked,
    });
  }
);

/* =====================================================
   LOGOUT
===================================================== */

app.post(
  "/api/logout",
  (req, res) => {
    req.session.destroy(() => {
      res.json({
        ok: true,
      });
    });
  }
);

/* =====================================================
   MATCHES
===================================================== */

app.post(
  "/api/unlock-matches",
  (req, res) => {
    if (
      !req.session.authenticated
    ) {
      return res
        .status(401)
        .json({
          ok: false,
          error:
            "Not authenticated.",
        });
    }

    const {
      code,
    } = req.body || {};

    if (
      String(code || "") !==
      MATCHES_CODE
    ) {
      return res
        .status(403)
        .json({
          ok: false,
          error:
            "Invalid activation code.",
        });
    }

    req.session.matchesUnlocked =
      true;

    res.json({
      ok: true,
      unlocked: true,
    });
  }
);

/* =====================================================
   DERIV DISCOVERY HELPER
===================================================== */

function discoverDerivMarkets() {
  return new Promise(
    (resolve, reject) => {
      let ws = null;
      let finished = false;

      const timeout =
        setTimeout(() => {
          if (finished) return;

          finished = true;

          try {
            if (ws) {
              ws.close();
            }
          } catch (_) {}

          reject(
            new Error(
              "Deriv market discovery timed out."
            )
          );
        }, 12000);

      try {
        ws =
          new WebSocket(
            DERIV_WS_URL
          );
      } catch (error) {
        clearTimeout(timeout);

        reject(error);

        return;
      }

      ws.on(
        "open",
        () => {
          try {
            ws.send(
              JSON.stringify({
                active_symbols:
                  "brief",

                req_id: 1,
              })
            );
          } catch (error) {
            if (finished) return;

            finished = true;

            clearTimeout(timeout);

            reject(error);
          }
        }
      );

      ws.on(
        "message",
        raw => {
          if (finished) return;

          let data;

          try {
            data =
              JSON.parse(
                raw.toString()
              );
          } catch (_) {
            return;
          }

          if (data.error) {
            finished = true;

            clearTimeout(timeout);

            try {
              ws.close();
            } catch (_) {}

            reject(
              new Error(
                data.error.message ||
                "Deriv discovery error."
              )
            );

            return;
          }

          if (
            data.msg_type ===
              "active_symbols"
          ) {
            finished = true;

            clearTimeout(timeout);

            const items =
              Array.isArray(
                data.active_symbols
              )
                ? data.active_symbols
                : [];

            const markets =
              discoveredMarkets(
                items
              );

            try {
              ws.close();
            } catch (_) {}

            resolve(markets);
          }
        }
      );

      ws.on(
        "error",
        error => {
          if (finished) return;

          finished = true;

          clearTimeout(timeout);

          reject(
            new Error(
              error?.message ||
              "Unable to connect to Deriv."
            )
          );
        }
      );

      ws.on(
        "close",
        () => {
          if (finished) return;

          finished = true;

          clearTimeout(timeout);

          reject(
            new Error(
              "Deriv closed the discovery connection before returning markets."
            )
          );
        }
      );
    }
  );
}

/* =====================================================
   API MARKET LIST
===================================================== */

app.get(
  "/api/markets",
  async (req, res) => {
    if (
      !req.session.authenticated
    ) {
      return res
        .status(401)
        .json({
          ok: false,
          error:
            "Not authenticated.",
        });
    }

    try {
      const markets =
        await discoverDerivMarkets();

      res.json({
        ok: true,
        markets,
        count: markets.length,
      });

    } catch (error) {
      console.error(
        "Market discovery error:",
        error
      );

      res
        .status(502)
        .json({
          ok: false,
          error:
            error?.message ||
            "Unable to discover Deriv markets.",
        });
    }
  }
);

/* =====================================================
   WEBSOCKET SESSION AUTHENTICATION
===================================================== */

server.on(
  "upgrade",
  (request, socket, head) => {

    if (
      !request.url ||
      !request.url.startsWith(
        "/live"
      )
    ) {
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
    };

    const fakeRequest = {
      headers:
        request.headers,

      connection:
        request.connection,

      socket:
        request.socket,

      url:
        request.url,
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
          ws => {

            ws.sessionData =
              sessionData;

            wss.emit(
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

/* =====================================================
   CLIENT WEBSOCKET
===================================================== */

wss.on(
  "connection",
  client => {

    let deriv = null;

    let selectedMarket = null;

    let selectedSymbol = null;

    let precision = null;

    let requestId = 100;

    let generation = 0;

    let intentionallyStopped =
      false;

    function send(payload) {
      if (
        client.readyState ===
        WebSocket.OPEN
      ) {
        try {
          client.send(
            JSON.stringify(
              payload
            )
          );
        } catch (_) {}
      }
    }

    function closeDeriv() {

      generation++;

      selectedMarket =
        null;

      selectedSymbol =
        null;

      precision =
        null;

      if (deriv) {
        try {
          deriv.removeAllListeners();
        } catch (_) {}

        try {
          deriv.close();
        } catch (_) {}

        deriv = null;
      }
    }

    async function startMarket(
      marketName,
      requestedSymbol
    ) {

      intentionallyStopped =
        false;

      const thisGeneration =
        ++generation;

      /*
        Close any previous Deriv
        connection before starting
        another market.
      */

      closeDeriv();

      /*
        closeDeriv() increments generation,
        therefore restore the generation
        belonging to this start request.
      */

      generation =
        thisGeneration + 1;

      const activeGeneration =
        generation;

      selectedMarket =
        marketName;

      send({
        type:
          "deriv_status",

        status:
          "discovering",

        marketName,

        message:
          "Checking the selected market with Deriv...",
      });

      let markets;

      try {

        markets =
          await discoverDerivMarkets();

      } catch (error) {

        if (
          activeGeneration !==
          generation
        ) {
          return;
        }

        send({
          type:
            "deriv_status",

          status:
            "error",

          marketName,

          message:
            error?.message ||
            "Deriv market discovery failed.",
        });

        selectedMarket =
          null;

        return;
      }

      if (
        activeGeneration !==
        generation
      ) {
        return;
      }

      /*
        Match by exact symbol first.
        Then exact market name.

        This prevents:
        Volatility 10 Index
        from being mixed with:
        Volatility 10 (1s)
      */

      let selected =
        null;

      if (requestedSymbol) {

        selected =
          markets.find(
            market =>
              market.symbol ===
              requestedSymbol &&
              normalize(
                market.name
              ) ===
                normalize(
                  marketName
                )
          );
      }

      if (!selected) {

        selected =
          markets.find(
            market =>
              normalize(
                market.name
              ) ===
                normalize(
                  marketName
                )
          );
      }

      if (!selected) {

        send({
          type:
            "deriv_status",

          status:
            "market_not_found",

          marketName,

          message:
            "The selected market is no longer active on Deriv.",
        });

        selectedMarket =
          null;

        return;
      }

      selectedMarket =
        selected.name;

      selectedSymbol =
        selected.symbol;

      precision =
        selected.precision;

      send({
        type:
          "market_confirmed",

        market:
          selected,
      });

      send({
        type:
          "deriv_status",

        status:
          "connecting",

        marketName:
          selected.name,

        symbol:
          selected.symbol,

        message:
          "Connecting to the exact Deriv market...",
      });

      /*
        Now open the actual live
        Deriv WebSocket.
      */

      let localDeriv;

      try {
        localDeriv =
          new WebSocket(
            DERIV_WS_URL
          );
      } catch (error) {

        send({
          type:
            "deriv_status",

          status:
            "error",

          marketName:
            selected.name,

          message:
            error?.message ||
            "Unable to create Deriv WebSocket.",
        });

        return;
      }

      deriv =
        localDeriv;

      const connectionGeneration =
        generation;

      localDeriv.on(
        "open",
        () => {

          if (
            connectionGeneration !==
            generation ||
            deriv !== localDeriv
          ) {
            try {
              localDeriv.close();
            } catch (_) {}

            return;
          }

          send({
            type:
              "deriv_status",

            status:
              "connected",

            marketName:
              selected.name,

            symbol:
              selected.symbol,

            message:
              "Deriv connection established.",
          });

          /*
            HISTORY FIRST
          */

          try {

            localDeriv.send(
              JSON.stringify({
                ticks_history:
                  selected.symbol,

                count: 60,

                end:
                  "latest",

                style:
                  "ticks",

                req_id:
                  requestId++,
              })
            );

          } catch (error) {

            send({
              type:
                "deriv_error",

              error:
                error?.message ||
                "Unable to request tick history.",
            });
          }

          /*
            THEN LIVE TICKS
          */

          try {

            localDeriv.send(
              JSON.stringify({
                ticks:
                  selected.symbol,

                subscribe:
                  1,

                req_id:
                  requestId++,
              })
            );

          } catch (error) {

            send({
              type:
                "deriv_error",

              error:
                error?.message ||
                "Unable to subscribe to live ticks.",
            });
          }

          send({
            type:
              "deriv_status",

            status:
              "live",

            marketName:
              selected.name,

            symbol:
              selected.symbol,

            message:
              "LIVE TICKS ACTIVE.",
          });
        }
      );

      localDeriv.on(
        "message",
        raw => {

          if (
            connectionGeneration !==
              generation ||
            deriv !==
              localDeriv
          ) {
            return;
          }

          let data;

          try {

            data =
              JSON.parse(
                raw.toString()
              );

          } catch (_) {

            send({
              type:
                "deriv_error",

              error:
                "Received invalid data from Deriv.",
            });

            return;
          }

          /* =========================
             DERIV ERROR
          ========================= */

          if (data.error) {

            send({
              type:
                "deriv_error",

              error:
                data.error.message ||
                "Deriv returned an error.",

              code:
                data.error.code ||
                "",
            });

            return;
          }

          /* =========================
             HISTORY
          ========================= */

          if (
            data.msg_type ===
            "history"
          ) {

            /*
              Current Deriv tick history
              normally provides prices
              and times.

              Send both to browser.
            */

            const prices =
              Array.isArray(
                data.history?.prices
              )
                ? data.history.prices
                : Array.isArray(
                    data.history
                  )
                ? data.history
                : [];

            const times =
              Array.isArray(
                data.history?.times
              )
                ? data.history.times
                : Array.isArray(
                    data.times
                  )
                ? data.times
                : [];

            send({
              type:
                "history",

              symbol:
                selected.symbol,

              marketName:
                selected.name,

              precision:
                precision,

              history:
                prices,

              times:
                times,
            });

            return;
          }

          /* =========================
             LIVE TICK
          ========================= */

          if (
            data.msg_type ===
              "tick" &&
            data.tick
          ) {

            /*
              Critical safety check.
            */

            if (
              data.tick.symbol !==
              selected.symbol
            ) {
              return;
            }

            send({
              type:
                "tick",

              marketName:
                selected.name,

              symbol:
                selected.symbol,

              precision:
                precision,

              tick: {
                symbol:
                  data.tick.symbol,

                quote:
                  data.tick.quote,

                epoch:
                  data.tick.epoch,

                pip_size:
                  data.tick.pip_size,
              },
            });

            return;
          }
        }
      );

      localDeriv.on(
        "error",
        error => {

          if (
            connectionGeneration !==
              generation
          ) {
            return;
          }

          console.error(
            "Deriv WebSocket error:",
            error
          );

          send({
            type:
              "deriv_status",

            status:
              "error",

            marketName:
              selected.name,

            symbol:
              selected.symbol,

            message:
              error?.message ||
              "Deriv WebSocket error.",
          });
        }
      );

      localDeriv.on(
        "close",
        (code, reason) => {

          if (
            connectionGeneration !==
              generation
          ) {
            return;
          }

          if (
            intentionallyStopped
          ) {
            return;
          }

          const reasonText =
            reason
              ? reason.toString()
              : "";

          send({
            type:
              "deriv_status",

            status:
              "closed",

            marketName:
              selected.name,

            symbol:
              selected.symbol,

            code,

            message:
              reasonText ||
              `Deriv WebSocket closed (code ${code}).`,
          });
        }
      );
    }

    /* =================================================
       CLIENT COMMANDS
    ================================================= */

    client.on(
      "message",
      raw => {

        let message;

        try {

          message =
            JSON.parse(
              raw.toString()
            );

        } catch (_) {

          send({
            type:
              "deriv_error",

            error:
              "Invalid command received.",
          });

          return;
        }

        /* =========================
           START
        ========================= */

        if (
          message.action ===
          "start"
        ) {

          const marketName =
            String(
              message.marketName ||
              ""
            ).trim();

          const symbol =
            String(
              message.symbol ||
              ""
            ).trim();

          if (!marketName) {

            send({
              type:
                "deriv_status",

              status:
                "error",

              message:
                "No market was selected.",
            });

            return;
          }

          startMarket(
            marketName,
            symbol
          );

          return;
        }

        /* =========================
           STOP
        ========================= */

        if (
          message.action ===
          "stop"
        ) {

          intentionallyStopped =
            true;

          closeDeriv();

          send({
            type:
              "deriv_status",

            status:
              "stopped",

            message:
              "Analyzer stopped.",
          });

          return;
        }
      }
    );

    /* =================================================
       CLIENT CLOSED
    ================================================= */

    client.on(
      "close",
      () => {

        intentionallyStopped =
          true;

        closeDeriv();
      }
    );
  }
);

/* =====================================================
   SERVER START
===================================================== */

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `DERIV LIVE ENTRY listening on port ${PORT}`
    );

    console.log(
      `Deriv WebSocket: ${DERIV_WS_URL}`
    );
  }
);
