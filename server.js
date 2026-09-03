/* =========================================================
   DERIV ACTIVE SYMBOL DISCOVERY
========================================================= */

let marketCache = [];
let marketCacheTime = 0;
let discoveryPromise = null;

/*
  Keep this reasonably long so Render does not repeatedly
  open new Deriv WebSocket connections.

  5 minutes = 300000 ms
*/
const MARKET_CACHE_TTL = 5 * 60 * 1000;


/* ---------------------------------------------------------
   Parse Deriv active_symbols response
--------------------------------------------------------- */

function discoverFromResponse(message) {
  if (!message) {
    throw new Error("Deriv returned an empty response.");
  }

  if (message.error) {
    throw new Error(
      message.error.message ||
      message.error.code ||
      "Deriv returned an unknown error."
    );
  }

  const data = message.data || message;

  let symbols = [];

  if (Array.isArray(data)) {
    symbols = data;
  } else if (Array.isArray(data.active_symbols)) {
    symbols = data.active_symbols;
  } else if (Array.isArray(message.active_symbols)) {
    symbols = message.active_symbols;
  }

  if (!symbols.length) {
    throw new Error(
      "Deriv responded, but no active_symbols array was found."
    );
  }

  const markets = symbols
    .filter(isWantedMarket)
    .filter(isMarketAvailable)
    .map(normalizeMarket)
    .filter(
      market =>
        market.symbol &&
        market.name
    );

  if (!markets.length) {
    throw new Error(
      "Deriv returned active markets, but no Volatility or Jump markets were found."
    );
  }

  /*
    Remove duplicate symbols.
  */

  const unique = new Map();

  for (const market of markets) {
    unique.set(market.symbol, market);
  }

  return sortMarkets(
    Array.from(unique.values())
  );
}


/* ---------------------------------------------------------
   Current public Deriv API
--------------------------------------------------------- */

function discoverUsingCurrentApi() {
  return new Promise((resolve, reject) => {
    let finished = false;

    const ws = new WebSocket(
      DERIV_PUBLIC_WS
    );

    const timeout = setTimeout(() => {
      if (finished) return;

      finished = true;

      try {
        ws.close();
      } catch (_) {}

      reject(
        new Error(
          "Timeout connecting to Deriv public API."
        )
      );
    }, 15000);


    ws.on("open", () => {
      console.log(
        "[DERIV] Current public API connected."
      );

      ws.send(
        JSON.stringify({
          active_symbols: "brief",
          req_id: 1001
        })
      );
    });


    ws.on("message", raw => {
      if (finished) return;

      try {
        const message =
          JSON.parse(raw.toString());

        console.log(
          "[DERIV] active_symbols response received."
        );

        if (message.error) {
          finished = true;
          clearTimeout(timeout);

          try {
            ws.close();
          } catch (_) {}

          reject(
            new Error(
              message.error.message ||
              message.error.code ||
              "Deriv API error."
            )
          );

          return;
        }


        /*
          Only process our request response.
        */

        if (
          message.req_id !== 1001 &&
          !message.active_symbols &&
          !message.data
        ) {
          return;
        }


        const markets =
          discoverFromResponse(message);

        finished = true;
        clearTimeout(timeout);

        try {
          ws.close();
        } catch (_) {}

        resolve(markets);

      } catch (error) {
        if (finished) return;

        finished = true;
        clearTimeout(timeout);

        try {
          ws.close();
        } catch (_) {}

        reject(error);
      }
    });


    ws.on("unexpected-response", (request, response) => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      const status =
        response.statusCode;

      console.error(
        `[DERIV] WebSocket HTTP error: ${status}`
      );

      if (status === 429) {
        reject(
          new Error(
            "Deriv rate-limited the public WebSocket connection (HTTP 429). Please wait and try again."
          )
        );
      } else {
        reject(
          new Error(
            `Deriv WebSocket rejected the connection with HTTP ${status}.`
          )
        );
      }
    });


    ws.on("error", error => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      reject(
        new Error(
          "Deriv public WebSocket error: " +
          error.message
        )
      );
    });


    ws.on("close", () => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      reject(
        new Error(
          "Deriv public WebSocket closed before returning markets."
        )
      );
    });
  });
}


/* ---------------------------------------------------------
   Legacy public API
--------------------------------------------------------- */

function discoverUsingLegacyApi() {
  return new Promise((resolve, reject) => {
    let finished = false;

    const appId =
      process.env.DERIV_APP_ID || "";

    let url =
      DERIV_LEGACY_WS;

    if (appId) {
      url +=
        "?app_id=" +
        encodeURIComponent(appId);
    }

    console.log(
      "[DERIV] Trying legacy public API..."
    );


    const ws =
      new WebSocket(url);


    const timeout =
      setTimeout(() => {
        if (finished) return;

        finished = true;

        try {
          ws.close();
        } catch (_) {}

        reject(
          new Error(
            "Timeout connecting to Deriv legacy API."
          )
        );
      }, 15000);


    ws.on("open", () => {
      console.log(
        "[DERIV] Legacy public API connected."
      );

      ws.send(
        JSON.stringify({
          active_symbols: "brief",
          product_type: "basic",
          req_id: 2001
        })
      );
    });


    ws.on("message", raw => {
      if (finished) return;

      try {
        const message =
          JSON.parse(raw.toString());

        if (message.error) {
          finished = true;
          clearTimeout(timeout);

          try {
            ws.close();
          } catch (_) {}

          reject(
            new Error(
              message.error.message ||
              message.error.code ||
              "Deriv legacy API error."
            )
          );

          return;
        }


        if (
          message.req_id === 2001 ||
          Array.isArray(
            message.active_symbols
          )
        ) {
          const markets =
            discoverFromResponse(message);

          finished = true;
          clearTimeout(timeout);

          try {
            ws.close();
          } catch (_) {}

          resolve(markets);
        }

      } catch (error) {
        if (finished) return;

        finished = true;
        clearTimeout(timeout);

        try {
          ws.close();
        } catch (_) {}

        reject(error);
      }
    });


    ws.on("unexpected-response", (request, response) => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      reject(
        new Error(
          `Legacy Deriv WebSocket HTTP ${response.statusCode}.`
        )
      );
    });


    ws.on("error", error => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      reject(
        new Error(
          "Legacy Deriv WebSocket error: " +
          error.message
        )
      );
    });


    ws.on("close", () => {
      if (finished) return;

      finished = true;
      clearTimeout(timeout);

      reject(
        new Error(
          "Legacy Deriv WebSocket closed before returning markets."
        )
      );
    });
  });
}


/* ---------------------------------------------------------
   Actual discovery
--------------------------------------------------------- */

async function performMarketDiscovery() {

  console.log(
    "[DERIV] Starting market discovery..."
  );


  /*
    First try the current public API.
  */

  try {

    const markets =
      await discoverUsingCurrentApi();

    console.log(
      `[DERIV] Current API returned ${markets.length} markets.`
    );

    return markets;

  } catch (currentError) {

    console.error(
      "[DERIV] Current API failed:",
      currentError.message
    );


    /*
      If Deriv says 429, DON'T immediately hammer
      the second endpoint.
    */

    if (
      currentError.message.includes("429") ||
      currentError.message
        .toLowerCase()
        .includes("rate-limit")
    ) {
      throw currentError;
    }


    /*
      Otherwise try legacy API.
    */

    try {

      const markets =
        await discoverUsingLegacyApi();

      console.log(
        `[DERIV] Legacy API returned ${markets.length} markets.`
      );

      return markets;

    } catch (legacyError) {

      console.error(
        "[DERIV] Legacy API failed:",
        legacyError.message
      );

      throw new Error(
        "Current API: " +
        currentError.message +
        " | Legacy API: " +
        legacyError.message
      );
    }
  }
}


/* ---------------------------------------------------------
   Cached discovery
--------------------------------------------------------- */

async function discoverDerivMarkets() {

  const now = Date.now();


  /*
    Return cached markets if still fresh.
  */

  if (
    marketCache.length > 0 &&
    now - marketCacheTime <
      MARKET_CACHE_TTL
  ) {

    console.log(
      `[DERIV] Using cached markets (${marketCache.length}).`
    );

    return marketCache;
  }


  /*
    If another request is already discovering markets,
    wait for that SAME request instead of opening another
    WebSocket.
  */

  if (discoveryPromise) {

    console.log(
      "[DERIV] Discovery already running; waiting for existing request."
    );

    return discoveryPromise;
  }


  discoveryPromise =
    performMarketDiscovery()
      .then(markets => {

        marketCache =
          markets;

        marketCacheTime =
          Date.now();

        console.log(
          `[DERIV] Market cache updated: ${markets.length} markets.`
        );

        return markets;
      })
      .finally(() => {

        discoveryPromise =
          null;
      });


  return discoveryPromise;
      }
