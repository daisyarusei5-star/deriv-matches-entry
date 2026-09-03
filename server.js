"use strict";

/*
=========================================================
DERIV LIVE ENTRY
POWERED BY ELISY 254

Analysis-only application.
NO AUTOMATIC TRADES ARE EXECUTED.

Expected project structure:

project/
├── server.js
├── index.html
├── package.json
└── package-lock.json

Environment variables:

PORT=3000
SESSION_SECRET=your_long_random_secret
LOGIN_MARKET=Market23
LOGIN_PASSWORD=Trade23
MATCHES_CODE=19809
DERIV_APP_ID=your_deriv_app_id

DERIV_APP_ID is only used for the legacy fallback
WebSocket endpoint. The current public market-data
endpoint does not require authentication/app_id.
=========================================================
*/


/* =========================================================
   DEPENDENCIES
========================================================= */

const express = require("express");
const session = require("express-session");
const http = require("http");
const path = require("path");
const WebSocket = require("ws");


/* =========================================================
   CONFIGURATION
========================================================= */

const app = express();

const server = http.createServer(app);

const PORT =
  Number(process.env.PORT) || 3000;

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

const DERIV_APP_ID =
  process.env.DERIV_APP_ID ||
  "";


/* =========================================================
   DERIV ENDPOINTS
========================================================= */

/*
  Current public Deriv market-data endpoint.
  No login/authentication is required for public
  market-data requests.
*/

const CURRENT_DERIV_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";


/*
  Legacy public endpoint.

  This is kept as a fallback because a Render outbound
  IP can temporarily receive rate limiting from the
  current endpoint.

  If DERIV_APP_ID is configured, it is appended to the
  legacy URL.
*/

const LEGACY_DERIV_WS =
  DERIV_APP_ID
    ? `wss://ws.binaryws.com/websockets/v3?app_id=${encodeURIComponent(DERIV_APP_ID)}`
    : "wss://ws.binaryws.com/websockets/v3";


/* =========================================================
   EXPRESS
========================================================= */

app.set("trust proxy", 1);

app.use(
  express.json({
    limit: "50kb"
  })
);

app.use(
  express.urlencoded({
    extended: false,
    limit: "50kb"
  })
);


/* =========================================================
   SESSION
========================================================= */

const sessionParser =
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
  });


app.use(sessionParser);


/* =========================================================
   STATIC FRONTEND
========================================================= */

/*
  IMPORTANT:

  This expects index.html to be in the SAME DIRECTORY
  as server.js.

  Render path should therefore be:

  /opt/render/project/src/index.html
*/

app.use(
  express.static(__dirname)
);


/* =========================================================
   ROOT PAGE
========================================================= */

app.get("/", (req, res) => {

  const indexPath =
    path.join(
      __dirname,
      "index.html"
    );

  res.sendFile(indexPath);
});


/* =========================================================
   HEALTH CHECK
========================================================= */

app.get("/health", (req, res) => {

  res.status(200).json({
    ok: true,
    service: "DERIV LIVE ENTRY",
    time: new Date().toISOString()
  });
});


/* =========================================================
   LOGIN
========================================================= */

app.post("/api/login", (req, res) => {

  const market =
    String(
      req.body?.market || ""
    ).trim();

  const password =
    String(
      req.body?.password || ""
    );

  if (
    !market ||
    !password
  ) {

    return res.status(400).json({
      ok: false,
      message:
        "Market and password are required."
    });
  }


  /*
    Constant-time style checks are not strictly necessary
    for this small application, but we avoid returning
    different information about which credential failed.
  */

  if (
    market !== LOGIN_MARKET ||
    password !== LOGIN_PASSWORD
  ) {

    return res.status(401).json({
      ok: false,
      message:
        "Invalid market or password."
    });
  }


  req.session.authenticated = true;

  req.session.loginMarket = market;

  req.session.matchesUnlocked = false;


  req.session.save((error) => {

    if (error) {

      console.error(
        "Session save error:",
        error
      );

      return res.status(500).json({
        ok: false,
        message:
          "Unable to create login session."
      });
    }


    return res.json({
      ok: true,
      message:
        "Login successful."
    });
  });
});


/* =========================================================
   SESSION CHECK
========================================================= */

app.get("/api/session", (req, res) => {

  return res.json({
    authenticated:
      req.session?.authenticated === true,

    matchesUnlocked:
      req.session?.matchesUnlocked === true
  });
});


/* =========================================================
   LOGOUT
========================================================= */

app.post("/api/logout", (req, res) => {

  req.session.destroy(() => {

    res.clearCookie(
      "connect.sid"
    );

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
  requireLogin,
  (req, res) => {

    const code =
      String(
        req.body?.code || ""
      ).trim();


    if (!code) {

      return res.status(400).json({
        ok: false,
        message:
          "Activation code is required."
      });
    }


    if (code !== MATCHES_CODE) {

      return res.status(401).json({
        ok: false,
        message:
          "Invalid activation code."
      });
    }


    req.session.matchesUnlocked = true;


    req.session.save((error) => {

      if (error) {

        console.error(
          "MATCHES session save error:",
          error
        );

        return res.status(500).json({
          ok: false,
          message:
            "Unable to save activation."
        });
      }


      return res.json({
        ok: true,
        message:
          "MATCHES unlocked."
      });
    });
  }
);


/* =========================================================
   LOGIN MIDDLEWARE
========================================================= */

function requireLogin(
  req,
  res,
  next
) {

  if (
    req.session &&
    req.session.authenticated === true
  ) {
    return next();
  }


  return res.status(401).json({
    ok: false,
    message:
      "Authentication required."
  });
}


/* =========================================================
   MARKET DISCOVERY CACHE
========================================================= */

/*
  Market discovery is intentionally cached.

  This is important because repeatedly opening a new
  Deriv WebSocket for active_symbols can cause rate
  limiting, especially from a shared Render IP.

  Cache duration:
  5 minutes.

  A single in-flight discovery request is also shared
  between simultaneous users.
*/

const MARKET_CACHE_TTL =
  5 * 60 * 1000;

let marketCache = null;

let marketCacheTime = 0;

let marketDiscoveryPromise = null;


/* =========================================================
   DERIV MARKET HELPERS
========================================================= */

function normalizeText(value) {

  return String(
    value ?? ""
  )
    .trim()
    .toLowerCase();
}


function isVolatilityOrJumpMarket(
  market
) {

  const combined = [
    market.symbol,
    market.display_name,
    market.underlying_symbol,
    market.underlying_symbol_name,
    market.name,
    market.market,
    market.submarket,
    market.subgroup
  ]
    .filter(Boolean)
    .join(" ");


  const text =
    normalizeText(combined);


  return (
    text.includes("volatility") ||
    text.includes("jump")
  );
}


/*
  Converts Deriv pip size to decimal precision.

  Examples:

  1      -> 0
  0.1    -> 1
  0.01   -> 2
  0.001  -> 3
*/

function precisionFromPip(
  pip
) {

  const number =
    Number(pip);

  if (
    !Number.isFinite(number) ||
    number <= 0
  ) {
    return 2;
  }


  let precision = 0;

  let value = number;


  while (
    precision < 10 &&
    Math.abs(
      value -
      Math.round(value)
    ) > 1e-12
  ) {

    value *= 10;

    precision++;
  }


  return precision;
}


/*
  Extract a normalized market object from both
  the current and legacy Deriv active_symbols
  response formats.
*/

function normalizeMarket(
  market
) {

  const symbol =
    String(
      market?.underlying_symbol ??
      market?.symbol ??
      ""
    ).trim();


  const name =
    String(
      market?.underlying_symbol_name ??
      market?.display_name ??
      market?.name ??
      symbol
    ).trim();


  const pipSize =
    Number(
      market?.pip_size ??
      market?.pip ??
      0.01
    );


  if (!symbol) {
    return null;
  }


  const precision =
    precisionFromPip(
      pipSize
    );


  return {
    symbol,
    name,
    precision,
    pipSize,

    market:
      market?.market ??
      "",

    submarket:
      market?.submarket ??
      "",

    subgroup:
      market?.subgroup ??
      "",

    exchangeIsOpen:
      market?.exchange_is_open,

    tradingSuspended:
      market?.is_trading_suspended
  };
}


/*
  Determine whether a market is currently usable.

  If Deriv does not provide these optional fields,
  we do not reject the market solely because they
  are missing.
*/

function isMarketTradable(
  market
) {

  if (
    market.exchangeIsOpen !== undefined &&
    market.exchangeIsOpen !== null
  ) {

    if (
      Number(
        market.exchangeIsOpen
      ) === 0
    ) {
      return false;
    }
  }


  if (
    market.tradingSuspended !== undefined &&
    market.tradingSuspended !== null
  ) {

    if (
      Number(
        market.tradingSuspended
      ) === 1
    ) {
      return false;
    }
  }


  return true;
}


/* =========================================================
   SORT MARKETS
========================================================= */

function sortMarkets(
  markets
) {

  return markets.sort(
    (a, b) => {

      const nameA =
        normalizeText(
          a.name
        );

      const nameB =
        normalizeText(
          b.name
        );


      /*
        Put Volatility markets first,
        then Jump markets.
      */

      const groupA =
        nameA.includes("volatility")
          ? 0
          : nameA.includes("jump")
            ? 1
            : 2;

      const groupB =
        nameB.includes("volatility")
          ? 0
          : nameB.includes("jump")
            ? 1
            : 2;


      if (groupA !== groupB) {
        return groupA - groupB;
      }


      return nameA.localeCompare(
        nameB,
        undefined,
        {
          numeric: true,
          sensitivity: "base"
        }
      );
    }
  );
}


/* =========================================================
   CONNECT TO DERIV FOR ACTIVE SYMBOLS
========================================================= */

function discoverMarketsFromEndpoint(
  endpoint,
  endpointName
) {

  return new Promise(
    (resolve, reject) => {

      let finished = false;

      let ws = null;

      let timeout = null;


      function finishError(
        error
      ) {

        if (finished) {
          return;
        }

        finished = true;


        if (timeout) {
          clearTimeout(timeout);
        }


        try {
          if (ws) {
            ws.close();
          }
        } catch {}


        const wrapped =
          new Error(
            `${endpointName}: ${error.message || error}`
          );


        wrapped.code =
          error.code;

        wrapped.statusCode =
          error.statusCode;

        wrapped.endpointName =
          endpointName;


        reject(wrapped);
      }


      function finishSuccess(
        markets
      ) {

        if (finished) {
          return;
        }

        finished = true;


        if (timeout) {
          clearTimeout(timeout);
        }


        try {
          if (ws) {
            ws.close();
          }
        } catch {}


        resolve(markets);
      }


      try {

        ws =
          new WebSocket(
            endpoint,
            {
              handshakeTimeout: 10000
            }
          );

      } catch (error) {

        finishError(error);

        return;
      }


      timeout =
        setTimeout(
          () => {

            const error =
              new Error(
                "Deriv connection timed out."
              );

            error.code =
              "DERIV_TIMEOUT";

            finishError(error);

          },
          15000
        );


      ws.on(
        "open",
        () => {

          /*
            Current API:
              active_symbols

            No product_type,
            landing_company,
            or authentication
            parameters are needed here.
          */

          const request = {
            active_symbols: "brief",
            req_id: 1001
          };


          try {

            ws.send(
              JSON.stringify(
                request
              )
            );

          } catch (error) {

            finishError(error);
          }
        }
      );


      ws.on(
        "message",
        (raw) => {

          let data;


          try {

            data =
              JSON.parse(
                raw.toString()
              );

          } catch (error) {

            finishError(
              new Error(
                "Invalid JSON received from Deriv."
              )
            );

            return;
          }


          /*
            Deriv error response.
          */

          if (data.error) {

            const message =
              String(
                data.error.message ||
                data.error.code ||
                "Deriv returned an error."
              );


            const error =
              new Error(
                message
              );


            error.code =
              data.error.code;


            /*
              Some ws libraries expose HTTP
              429 during handshake rather than
              as a normal WebSocket message.
            */

            if (
              String(
                data.error.code || ""
              ).includes("429")
            ) {
              error.statusCode = 429;
            }


            finishError(error);

            return;
          }


          /*
            Current/legacy active_symbols
            response is generally an array.
          */

          if (
            Array.isArray(
              data.active_symbols
            )
          ) {

            const normalized =
              data.active_symbols
                .map(
                  normalizeMarket
                )
                .filter(Boolean)
                .filter(
                  isVolatilityOrJumpMarket
                )
                .filter(
                  isMarketTradable
                );


            finishSuccess(
              normalized
            );

            return;
          }


          /*
            Some API responses can arrive in
            slightly different envelopes.
          */

          if (
            Array.isArray(
              data.activeSymbols
            )
          ) {

            const normalized =
              data.activeSymbols
                .map(
                  normalizeMarket
                )
                .filter(Boolean)
                .filter(
                  isVolatilityOrJumpMarket
                )
                .filter(
                  isMarketTradable
                );


            finishSuccess(
              normalized
            );

            return;
          }
        }
      );


      ws.on(
        "unexpected-response",
        (
          request,
          response
        ) => {

          const status =
            Number(
              response.statusCode
            );


          const error =
            new Error(
              `Unexpected server response: ${status}`
            );


          error.statusCode =
            status;


          error.code =
            `HTTP_${status}`;


          finishError(error);
        }
      );


      ws.on(
        "error",
        (error) => {

          finishError(error);
        }
      );


      ws.on(
        "close",
        (
          code,
          reason
        ) => {

          if (finished) {
            return;
          }


          const reasonText =
            reason
              ? reason.toString()
              : "";


          const error =
            new Error(
              `Deriv WebSocket closed before market discovery completed. Code ${code}${reasonText ? `: ${reasonText}` : ""}`
            );


          error.code =
            code;


          finishError(error);
        }
      );
    }
  );
}


/* =========================================================
   DISCOVER MARKETS
========================================================= */

async function discoverMarkets() {

  /*
    Return valid cache immediately.
  */

  if (
    marketCache &&
    Date.now() -
      marketCacheTime <
      MARKET_CACHE_TTL
  ) {

    return {
      markets: marketCache,
      cached: true,
      source: "cache"
    };
  }


  /*
    If another request is already discovering markets,
    wait for that exact same request instead of opening
    another Deriv WebSocket.
  */

  if (marketDiscoveryPromise) {
    return marketDiscoveryPromise;
  }


  marketDiscoveryPromise =
    (async () => {

      let currentError =
        null;


      /*
        Attempt 1:
        Current public endpoint.
      */

      try {

        console.log(
          "Market discovery: connecting to current Deriv public endpoint..."
        );


        const markets =
          await discoverMarketsFromEndpoint(
            CURRENT_DERIV_WS,
            "Current Deriv API"
          );


        if (
          Array.isArray(markets) &&
          markets.length > 0
        ) {

          const unique =
            deduplicateMarkets(
              markets
            );


          sortMarkets(unique);


          marketCache =
            unique;

          marketCacheTime =
            Date.now();


          console.log(
            `Market discovery successful: ${unique.length} Volatility/Jump markets found.`
          );


          return {
            markets: unique,
            cached: false,
            source: "current"
          };
        }


        /*
          Empty response is not necessarily a hard
          connection failure. Try fallback.
        */

        currentError =
          new Error(
            "Current Deriv API returned no matching Volatility/Jump markets."
          );


        console.warn(
          currentError.message
        );

      } catch (error) {

        currentError =
          error;


        console.error(
          "Current Deriv market discovery failed:",
          error.message
        );
      }


      /*
        Small delay before fallback.

        This prevents an immediate second connection
        hammering Deriv after a rate-limit response.
      */

      await sleep(1200);


      /*
        Attempt 2:
        Legacy public endpoint.

        If no app ID is configured, the endpoint can still
        be attempted, but some legacy configurations may
        require an app ID.
      */

      try {

        console.log(
          "Market discovery: trying legacy Deriv public endpoint..."
        );


        const markets =
          await discoverMarketsFromEndpoint(
            LEGACY_DERIV_WS,
            "Legacy Deriv API"
          );


        if (
          Array.isArray(markets) &&
          markets.length > 0
        ) {

          const unique =
            deduplicateMarkets(
              markets
            );


          sortMarkets(unique);


          marketCache =
            unique;

          marketCacheTime =
            Date.now();


          console.log(
            `Legacy market discovery successful: ${unique.length} Volatility/Jump markets found.`
          );


          return {
            markets: unique,
            cached: false,
            source: "legacy"
          };
        }


        throw new Error(
          "Legacy Deriv API returned no matching Volatility/Jump markets."
        );

      } catch (legacyError) {

        console.error(
          "Legacy Deriv market discovery failed:",
          legacyError.message
        );


        /*
          Preserve the most useful error.
        */

        const finalError =
          new Error(
            `Unable to discover Deriv markets. Current API: ${currentError?.message || "failed"}. Legacy API: ${legacyError.message || "failed"}.`
          );


        finalError.currentError =
          currentError;

        finalError.legacyError =
          legacyError;


        throw finalError;
      }

    })()
      .finally(
        () => {
          marketDiscoveryPromise =
            null;
        }
      );


  return marketDiscoveryPromise;
}


/* =========================================================
   DEDUPLICATE MARKETS
========================================================= */

function deduplicateMarkets(
  markets
) {

  const map =
    new Map();


  for (const market of markets) {

    if (
      !market ||
      !market.symbol
    ) {
      continue;
    }


    /*
      Symbol is the single source of truth.
    */

    if (
      !map.has(
        market.symbol
      )
    ) {

      map.set(
        market.symbol,
        market
      );
    }
  }


  return Array.from(
    map.values()
  );
}


/* =========================================================
   SLEEP
========================================================= */

function sleep(
  milliseconds
) {

  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        milliseconds
      )
  );
}


/* =========================================================
   MARKETS API
========================================================= */

app.get(
  "/api/markets",
  requireLogin,
  async (req, res) => {

    try {

      const result =
        await discoverMarkets();


      return res.json({
        ok: true,

        markets:
          result.markets,

        cached:
          result.cached,

        source:
          result.source,

        count:
          result.markets.length,

        cacheAgeMs:
          marketCacheTime
            ? Date.now() -
              marketCacheTime
            : 0
      });

    } catch (error) {

      console.error(
        "Market discovery error:",
        error
      );


      const status =
        Number(
          error?.statusCode
        ) === 429
          ? 503
          : 500;


      return res.status(status).json({
        ok: false,

        message:
          "Deriv market discovery is temporarily unavailable.",

        error:
          error.message ||
          "Unknown market discovery error."
      });
    }
  }
);


/* =========================================================
   EXACT MARKET MATCHING
========================================================= */

function normalizeMarketName(
  value
) {

  return String(
    value ?? ""
  )
    .trim()
    .replace(
      /\s+/g,
      " "
    )
    .toLowerCase();
}


function marketNamesMatch(
  a,
  b
) {

  return (
    normalizeMarketName(a) ===
    normalizeMarketName(b)
  );
}


/* =========================================================
   SERVER-SIDE DERIV MARKET CONNECTION
========================================================= */

class DerivMarketConnection {

  constructor(
    browserSocket
  ) {

    this.browserSocket =
      browserSocket;

    this.derivSocket =
      null;

    this.started =
      false;

    this.closed =
      false;

    this.symbol =
      "";

    this.marketName =
      "";

    this.precision =
      2;

    this.requestId =
      5000;

    this.historyReceived =
      false;
  }


  nextReqId() {

    this.requestId += 1;

    return this.requestId;
  }


  sendBrowser(
    payload
  ) {

    if (
      !this.browserSocket ||
      this.browserSocket.readyState !==
        WebSocket.OPEN
    ) {
      return;
    }


    try {

      this.browserSocket.send(
        JSON.stringify(
          payload
        )
      );

    } catch (error) {

      console.error(
        "Browser WebSocket send error:",
        error.message
      );
    }
  }


  async startMarket(
    marketName,
    symbol
  ) {

    if (
      this.started
    ) {
      this.stop();
    }


    this.started =
      true;

    this.closed =
      false;

    this.symbol =
      String(
        symbol || ""
      ).trim();

    this.marketName =
      String(
        marketName || ""
      ).trim();


    if (
      !this.symbol ||
      !this.marketName
    ) {

      this.sendBrowser({
        type: "deriv_error",
        message:
          "Market symbol and market name are required."
      });

      return;
    }


    this.sendBrowser({
      type: "deriv_status",
      status: "discovering"
    });


    /*
      Verify the selected market against our
      cached/dynamic market list.

      This ensures the server never blindly trusts
      arbitrary symbols sent by the browser.
    */

    let marketsResult;


    try {

      marketsResult =
        await discoverMarkets();

    } catch (error) {

      this.sendBrowser({
        type: "deriv_error",
        message:
          "Unable to verify selected Deriv market: " +
          error.message
      });

      return;
    }


    const markets =
      marketsResult.markets;


    const exact =
      markets.find(
        market =>
          market.symbol ===
            this.symbol &&
          marketNamesMatch(
            market.name,
            this.marketName
          )
      );


    if (!exact) {

      /*
        Try symbol-only matching once.

        The symbol remains authoritative.
        The name is used as a consistency check,
        but Deriv can occasionally change display
        formatting.
      */

      const symbolMatch =
        markets.find(
          market =>
            market.symbol ===
            this.symbol
        );


      if (!symbolMatch) {

        this.sendBrowser({
          type: "market_not_found",

          symbol:
            this.symbol,

          marketName:
            this.marketName,

          message:
            "Selected market is no longer available on Deriv."
        });

        return;
      }


      /*
        Symbol exists but display name differs.

        Adopt the server's current official name.
      */

      this.marketName =
        symbolMatch.name;

      this.precision =
        Number(
          symbolMatch.precision ?? 2
        );

    } else {

      this.marketName =
        exact.name;

      this.precision =
        Number(
          exact.precision ?? 2
        );
    }


    this.sendBrowser({
      type: "market_confirmed",

      symbol:
        this.symbol,

      marketName:
        this.marketName,

      precision:
        this.precision
    });


    await this.connectToDeriv();
  }


  async connectToDeriv() {

    if (
      this.closed ||
      !this.started
    ) {
      return;
    }


    this.sendBrowser({
      type: "deriv_status",
      status: "connecting"
    });


    /*
      Use the current public endpoint for the actual
      market data connection.
    */

    try {

      await this.openDerivSocket(
        CURRENT_DERIV_WS
      );

    } catch (currentError) {

      console.error(
        "Current Deriv data connection failed:",
        currentError.message
      );


      /*
        Fallback to legacy endpoint.
      */

      try {

        await sleep(1000);


        await this.openDerivSocket(
          LEGACY_DERIV_WS
        );

      } catch (legacyError) {

        console.error(
          "Legacy Deriv data connection failed:",
          legacyError.message
        );


        this.sendBrowser({
          type: "deriv_error",

          message:
            "Unable to connect to Deriv live market data. " +
            `Current API: ${currentError.message}. ` +
            `Legacy API: ${legacyError.message}.`
        });
      }
    }
  }


  openDerivSocket(
    endpoint
  ) {

    return new Promise(
      (resolve, reject) => {

        let settled =
          false;

        let timeout =
          null;


        let ws;


        const finishReject =
          (error) => {

            if (settled) {
              return;
            }

            settled = true;


            if (timeout) {
              clearTimeout(
                timeout
              );
            }


            try {
              if (ws) {
                ws.close();
              }
            } catch {}


            reject(error);
          };


        const finishResolve =
          () => {

            if (settled) {
              return;
            }

            settled = true;


            if (timeout) {
              clearTimeout(
                timeout
              );
            }


            resolve();
          };


        try {

          ws =
            new WebSocket(
              endpoint,
              {
                handshakeTimeout:
                  10000
              }
            );

        } catch (error) {

          finishReject(error);

          return;
        }


        this.derivSocket =
          ws;


        timeout =
          setTimeout(
            () => {

              const error =
                new Error(
                  "Deriv WebSocket connection timed out."
                );

              error.code =
                "DERIV_TIMEOUT";

              finishReject(error);

            },
            15000
          );


        ws.on(
          "open",
          () => {

            /*
              First request history.

              ticks_history is public market data.
            */

            const historyRequest = {
              ticks_history:
                this.symbol,

              count: 100,

              end: "latest",

              style: "ticks",

              req_id:
                this.nextReqId()
            };


            try {

              ws.send(
                JSON.stringify(
                  historyRequest
                )
              );


              /*
                Then subscribe to live ticks.
              */

              const tickRequest = {
                ticks:
                  this.symbol,

                subscribe: 1,

                req_id:
                  this.nextReqId()
              };


              ws.send(
                JSON.stringify(
                  tickRequest
                )
              );


              this.sendBrowser({
                type: "deriv_status",
                status: "connected"
              });


              finishResolve();

            } catch (error) {

              finishReject(error);
            }
          }
        );


        ws.on(
          "message",
          (raw) => {

            this.handleDerivMessage(
              raw
            );
          }
        );


        ws.on(
          "unexpected-response",
          (
            request,
            response
          ) => {

            const error =
              new Error(
                `Unexpected server response: ${response.statusCode}`
              );


            error.statusCode =
              response.statusCode;


            error.code =
              `HTTP_${response.statusCode}`;


            finishReject(error);
          }
        );


        ws.on(
          "error",
          (error) => {

            /*
              If the connection had already been
              established, report the error to browser.
            */

            if (settled) {

              this.sendBrowser({
                type: "deriv_error",

                message:
                  error.message ||
                  "Deriv WebSocket error."
              });

              return;
            }


            finishReject(error);
          }
        );


        ws.on(
          "close",
          (
            code,
            reason
          ) => {

            if (!settled) {

              const error =
                new Error(
                  `Deriv WebSocket closed during connection. Code ${code}`
                );


              error.code =
                code;


              finishReject(error);

              return;
            }


            if (
              !this.closed &&
              this.started
            ) {

              this.sendBrowser({
                type: "closed",

                code,

                reason:
                  reason
                    ? reason.toString()
                    : ""
              });
            }
          }
        );
      }
    );
  }


  handleDerivMessage(
    raw
  ) {

    let data;


    try {

      data =
        JSON.parse(
          raw.toString()
        );

    } catch (error) {

      console.error(
        "Invalid Deriv message:",
        error.message
      );

      return;
    }


    /*
      Deriv error.
    */

    if (data.error) {

      const errorMessage =
        String(
          data.error.message ||
          data.error.code ||
          "Deriv returned an error."
        );


      console.error(
        "Deriv API error:",
        data.error
      );


      this.sendBrowser({
        type: "deriv_error",

        code:
          data.error.code,

        message:
          errorMessage
      });


      return;
    }


    /*
      Tick history.
    */

    if (
      data.history
    ) {

      /*
        Legacy/current responses may expose
        history as an array of quote strings.
      */

      const history =
        Array.isArray(
          data.history.prices
        )
          ? data.history.prices
          : Array.isArray(
              data.history
            )
              ? data.history
              : [];


      const times =
        Array.isArray(
          data.history.times
        )
          ? data.history.times
          : Array.isArray(
              data.times
            )
              ? data.times
              : [];


      /*
        If the response identifies a symbol,
        enforce exact symbol isolation.
      */

      const responseSymbol =
        data.echo_req?.ticks_history ||
        data.echo_req?.symbol ||
        data.symbol ||
        this.symbol;


      if (
        responseSymbol &&
        responseSymbol !==
          this.symbol
      ) {

        console.warn(
          "Ignoring history from wrong symbol:",
          responseSymbol
        );

        return;
      }


      this.historyReceived =
        true;


      this.sendBrowser({
        type: "history",

        symbol:
          this.symbol,

        marketName:
          this.marketName,

        precision:
          this.precision,

        history,

        times
      });


      return;
    }


    /*
      Live tick.

      Depending on API response, the tick symbol can
      appear in tick.symbol or echo_req.ticks.
    */

    if (
      data.tick
    ) {

      const tickSymbol =
        data.tick.symbol ||
        data.echo_req?.ticks ||
        data.echo_req?.symbol ||
        "";


      /*
        HARD SYMBOL ISOLATION.

        Never forward a tick to the browser if it
        belongs to another market.
      */

      if (
        tickSymbol &&
        tickSymbol !==
          this.symbol
      ) {

        console.warn(
          "Ignoring live tick from wrong symbol:",
          tickSymbol
        );

        return;
      }


      if (
        !data.tick.quote
      ) {
        return;
      }


      this.sendBrowser({
        type: "tick",

        symbol:
          this.symbol,

        marketName:
          this.marketName,

        precision:
          this.precision,

        tick: {
          quote:
            data.tick.quote,

          epoch:
            data.tick.epoch ??
            null
        }
      });


      return;
    }


    /*
      Subscription confirmation.
    */

    if (
      data.subscription
    ) {

      this.sendBrowser({
        type: "deriv_status",
        status: "live"
      });


      return;
    }
  }


  stop() {

    this.started =
      false;


    this.closed =
      true;


    if (this.derivSocket) {

      try {

        this.derivSocket.close();

      } catch {}
    }


    this.derivSocket =
      null;


    this.sendBrowser({
      type: "deriv_status",
      status: "stopped"
    });
  }


  destroy() {

    this.stop();

    this.browserSocket =
      null;
  }
}


/* =========================================================
   WEBSOCKET SERVER
========================================================= */

const wsServer =
  new WebSocket.Server({
    noServer: true
  });


/* =========================================================
   HTTP -> WEBSOCKET UPGRADE
========================================================= */

server.on(
  "upgrade",
  (req, socket, head) => {

    /*
      Only /live is accepted as a WebSocket route.
    */

    const pathname =
      String(
        req.url || ""
      ).split("?")[0];


    if (
      pathname !== "/live"
    ) {

      socket.destroy();

      return;
    }


    /*
      Parse the existing Express session
      from the WebSocket handshake.
    */

    sessionParser(
      req,
      {},
      (sessionError) => {

        if (sessionError) {

          console.error(
            "WebSocket session error:",
            sessionError
          );

          socket.write(
            "HTTP/1.1 500 Internal Server Error\r\n\r\n"
          );

          socket.destroy();

          return;
        }


        /*
          Require login before allowing the browser
          to establish the /live WebSocket.
        */

        if (
          !req.session ||
          req.session.authenticated !== true
        ) {

          socket.write(
            "HTTP/1.1 401 Unauthorized\r\n\r\n"
          );

          socket.destroy();

          return;
        }


        wsServer.handleUpgrade(
          req,
          socket,
          head,
          (ws) => {

            wsServer.emit(
              "connection",
              ws,
              req
            );
          }
        );
      }
    );
  }
);


/* =========================================================
   BROWSER WEBSOCKET CONNECTION
========================================================= */

wsServer.on(
  "connection",
  (browserSocket, req) => {

    console.log(
      "Browser /live WebSocket connected."
    );


    const connection =
      new DerivMarketConnection(
        browserSocket
      );


    browserSocket.on(
      "message",
      async (raw) => {

        let data;


        try {

          data =
            JSON.parse(
              raw.toString()
            );

        } catch (error) {

          connection.sendBrowser({
            type: "deriv_error",

            message:
              "Invalid browser message."
          });

          return;
        }


        if (!data.action) {
          return;
        }


        /*
          START

          Browser sends:

          {
            action: "start",
            marketName: "...",
            symbol: "..."
          }
        */

        if (
          data.action === "start"
        ) {

          const marketName =
            String(
              data.marketName || ""
            ).trim();


          const symbol =
            String(
              data.symbol || ""
            ).trim();


          if (
            !marketName ||
            !symbol
          ) {

            connection.sendBrowser({
              type: "deriv_error",

              message:
                "A valid Deriv market and symbol are required."
            });

            return;
          }


          try {

            await connection.startMarket(
              marketName,
              symbol
            );

          } catch (error) {

            console.error(
              "START market error:",
              error
            );


            connection.sendBrowser({
              type: "deriv_error",

              message:
                error.message ||
                "Unable to start market."
            });
          }


          return;
        }


        /*
          STOP
        */

        if (
          data.action === "stop"
        ) {

          connection.stop();

          return;
        }
      }
    );


    browserSocket.on(
      "close",
      () => {

        console.log(
          "Browser /live WebSocket disconnected."
        );


        connection.destroy();
      }
    );


    browserSocket.on(
      "error",
      (error) => {

        console.error(
          "Browser WebSocket error:",
          error.message
        );


        connection.destroy();
      }
    );


    /*
      IMPORTANT:

      We DO NOT start Deriv discovery here.

      The browser must first send an explicit
      START command containing the selected symbol.

      This prevents the old __DISCOVERY_ONLY__
      problem and prevents unnecessary Deriv
      connections.
    */

    connection.sendBrowser({
      type: "deriv_status",
      status: "ready"
    });
  }
);


/* =========================================================
   404 API HANDLER
========================================================= */

app.use(
  "/api",
  (req, res) => {

    return res.status(404).json({
      ok: false,
      message:
        "API endpoint not found."
    });
  }
);


/* =========================================================
   GENERAL ERROR HANDLER
========================================================= */

app.use(
  (error, req, res, next) => {

    console.error(
      "Express error:",
      error
    );


    if (
      res.headersSent
    ) {
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

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      "================================================="
    );

    console.log(
      "DERIV LIVE ENTRY SERVER"
    );

    console.log(
      "POWERED BY ELISY 254"
    );

    console.log(
      "================================================="
    );

    console.log(
      `Server listening on port ${PORT}`
    );

    console.log(
      `Frontend expected at: ${path.join(__dirname, "index.html")}`
    );

    console.log(
      `Current Deriv endpoint: ${CURRENT_DERIV_WS}`
    );

    console.log(
      `Legacy Deriv fallback configured: ${DERIV_APP_ID ? "YES" : "NO"}`
    );

    console.log(
      "Market discovery cache: 5 minutes"
    );

    console.log(
      "Automatic trading: DISABLED"
    );

    console.log(
      "================================================="
    );
  }
);


/* =========================================================
   PROCESS ERROR HANDLERS
========================================================= */

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
  (error) => {

    console.error(
      "UNHANDLED REJECTION:",
      error
    );
  }
);


/* =========================================================
   GRACEFUL SHUTDOWN
========================================================= */

function shutdown(
  signal
) {

  console.log(
    `${signal} received. Shutting down...`
  );


  server.close(
    () => {

      console.log(
        "HTTP server closed."
      );

      process.exit(0);
    }
  );


  /*
    Safety timeout.
  */

  setTimeout(
    () => {
      process.exit(0);
    },
    10000
  );
}


process.on(
  "SIGTERM",
  () => shutdown("SIGTERM")
);

process.on(
  "SIGINT",
  () => shutdown("SIGINT")
);
