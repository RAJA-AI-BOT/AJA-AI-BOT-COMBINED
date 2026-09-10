package ai.raja.bridge;

import com.dukascopy.api.*;
import com.dukascopy.api.system.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class Main {

  static final String DEMO_URL =
      "http://platform.dukascopy.com/demo_3/jforex_3.jnlp";

  static final Map<String, Instrument> PAIRS = new LinkedHashMap<>();
  static final ConcurrentHashMap<String, Deque<Candle>> candles =
      new ConcurrentHashMap<>();

  // V80 live-tick state. Each entry is immutable and replaced atomically on
  // every Dukascopy BID tick so HTTP readers never see a half-updated candle.
  static final ConcurrentHashMap<String, TickState> liveTicks =
      new ConcurrentHashMap<>();
  static final ConcurrentHashMap<Instrument, String> INSTRUMENT_PAIRS =
      new ConcurrentHashMap<>();

  static volatile boolean connected = false;
  static volatile String lastError = "";
  static volatile long lastBarAt = 0;
  static volatile long lastTickAt = 0;

  static IClient client;

  // Broad standard Forex set. Unsupported symbols are skipped safely.
  static final String[] DEFAULT_FOREX_PAIRS = {
      "EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","USD/CAD","NZD/USD",
      "EUR/JPY","GBP/JPY","CAD/JPY","AUD/JPY","CHF/JPY","NZD/JPY",
      "EUR/GBP","EUR/AUD","EUR/CAD","EUR/CHF","EUR/NZD",
      "GBP/AUD","GBP/CAD","GBP/CHF","GBP/NZD",
      "AUD/CAD","AUD/CHF","AUD/NZD",
      "CAD/CHF","NZD/CAD","NZD/CHF",
      "USD/SGD","USD/HKD","USD/MXN","USD/ZAR","USD/TRY","USD/PLN","USD/CNH",
      "EUR/PLN","EUR/TRY","EUR/SEK","EUR/NOK","EUR/DKK",
      "GBP/SGD","GBP/SEK","GBP/NOK",
      "AUD/SGD","NZD/SGD","CAD/SGD","CHF/SGD",
      "SGD/JPY","HKD/JPY","ZAR/JPY","TRY/JPY",
      "USD/SEK","USD/NOK","USD/DKK"
  };

  static {
    loadPairs();
  }

  static void loadPairs() {
    // Optional Railway override:
    // DUKASCOPY_PAIRS=EUR/USD,GBP/USD,USD/JPY,...
    String env = System.getenv("DUKASCOPY_PAIRS");

    String[] requested =
        (env != null && !env.isBlank())
            ? env.split(",")
            : DEFAULT_FOREX_PAIRS;

    for (String raw : requested) {
      String pair = normalizePair(raw);
      try {
        Instrument instrument = Instrument.fromString(pair);
        if (instrument != null) {
          PAIRS.put(pair, instrument);
          INSTRUMENT_PAIRS.put(instrument, pair);
          candles.put(pair, new ConcurrentLinkedDeque<>());
        }
      } catch (Exception ignored) {
        // Unsupported pair: skip it instead of killing the bridge.
      }
    }
  }

  static String normalizePair(String pair) {
    if (pair == null) return "";
    String p = pair.trim().toUpperCase(Locale.ROOT)
        .replace("_", "/")
        .replace("-", "/");

    if (!p.contains("/") && p.length() == 6) {
      p = p.substring(0, 3) + "/" + p.substring(3);
    }
    return p;
  }

  public static void main(String[] args) throws Exception {
    int port =
        Integer.parseInt(System.getenv().getOrDefault("BRIDGE_PORT", "8081"));

    startHttp(port);

    String username = System.getenv("DUKASCOPY_USERNAME");
    String password = System.getenv("DUKASCOPY_PASSWORD");

    if (username == null || username.isBlank()
        || password == null || password.isBlank()) {
      lastError =
          "DUKASCOPY_USERNAME / DUKASCOPY_PASSWORD missing";
      return;
    }

    connect(username, password);
  }

  static void connect(String username, String password) throws Exception {
    client = ClientFactory.getDefaultInstance();

    client.setSystemListener(new ISystemListener() {
      public void onStart(long processId) {}
      public void onStop(long processId) {}

      public void onConnect() {
        connected = true;
        lastError = "";
      }

      public void onDisconnect() {
        connected = false;
      }
    });

    client.connect(DEMO_URL, username, password);

    for (int i = 0; i < 30 && !client.isConnected(); i++) {
      Thread.sleep(1000);
    }

    if (!client.isConnected()) {
      lastError = "Dukascopy demo login did not connect";
      return;
    }

    // Current JForex API signature takes only Set<Instrument>.
    client.setSubscribedInstruments(
        new HashSet<>(PAIRS.values())
    );

    client.startStrategy(new FeedStrategy());
  }

  public static class FeedStrategy implements IStrategy {

    IHistory history;

    public void onStart(IContext context) throws JFException {
      history = context.getHistory();

      // Give subscriptions/cache a moment to initialize.
      try {
        Thread.sleep(1500);
      } catch (InterruptedException ignored) {}

      for (Map.Entry<String, Instrument> entry : PAIRS.entrySet()) {
        preloadHistory(entry.getKey(), entry.getValue());
      }
    }

    void preloadHistory(String pair, Instrument instrument) {
      try {
        /*
         * Important fix:
         * getBars(...numberBefore, time, numberAfter) expects "time"
         * to match a bar boundary. Using raw System.currentTimeMillis()
         * caused:
         * "time is not correct time for the period specified".
         *
         * We anchor history to the PREVIOUS CLOSED 1-minute bar.
         */
        long sourceTime;

        try {
          ITick lastTick = history.getLastTick(instrument);
          sourceTime =
              (lastTick != null && lastTick.getTime() > 0)
                  ? lastTick.getTime()
                  : System.currentTimeMillis();
        } catch (Exception e) {
          sourceTime = System.currentTimeMillis();
        }

        long lastClosedBarTime =
            history.getPreviousBarStart(
                Period.ONE_MIN,
                sourceTime
            );

        List<IBar> bars =
            history.getBars(
                instrument,
                Period.ONE_MIN,
                OfferSide.BID,
                Filter.NO_FILTER,
                500,
                lastClosedBarTime,
                0
            );

        for (IBar bar : bars) {
          put(pair, bar);
        }

      } catch (Exception e) {
        // Keep bridge alive even if one exotic pair has no history.
        lastError =
            "History " + pair + ": "
            + (e.getMessage() == null
                ? e.getClass().getSimpleName()
                : e.getMessage());
      }
    }

    public void onBar(
        Instrument instrument,
        Period period,
        IBar askBar,
        IBar bidBar
    ) {
      if (period != Period.ONE_MIN || bidBar == null) {
        return;
      }

      for (Map.Entry<String, Instrument> entry : PAIRS.entrySet()) {
        if (entry.getValue().equals(instrument)) {
          put(entry.getKey(), bidBar);
          break;
        }
      }
    }

    public void onTick(Instrument instrument, ITick tick) {
      if (instrument == null || tick == null) return;
      String pair = INSTRUMENT_PAIRS.get(instrument);
      if (pair == null) return;

      try {
        long tickTime = tick.getTime() > 0 ? tick.getTime() : System.currentTimeMillis();
        double bid = tick.getBid();
        double ask = tick.getAsk();
        if (!Double.isFinite(bid) || bid <= 0.0) return;

        long minuteStart = (tickTime / 60000L) * 60000L;
        TickState prev = liveTicks.get(pair);
        TickState next;

        // V80 candle integrity: never let a delayed/out-of-order network tick
        // rewind the current forming candle or corrupt its high/low.
        if (prev != null && tickTime <= prev.t) return;
        if (prev != null && minuteStart < prev.minuteStart) return;

        if (prev == null || minuteStart > prev.minuteStart) {
          next = new TickState(
              tickTime, minuteStart, bid, ask,
              bid, bid, bid, bid
          );
        } else {
          next = new TickState(
              tickTime, minuteStart, bid, ask,
              prev.o,
              Math.max(prev.h, bid),
              Math.min(prev.l, bid),
              bid
          );
        }

        liveTicks.put(pair, next);
        lastTickAt = System.currentTimeMillis();
      } catch (Exception ignored) {
        // A malformed/temporary tick must never stop the strategy callback.
      }
    }
    public void onMessage(IMessage message) {}
    public void onAccount(IAccount account) {}
    public void onStop() {}
  }

  static void put(String pair, IBar bar) {
    Deque<Candle> q = candles.get(pair);
    if (q == null) return;

    Candle candle =
        new Candle(
            bar.getTime(),
            bar.getOpen(),
            bar.getHigh(),
            bar.getLow(),
            bar.getClose()
        );

    synchronized (q) {
      Candle last = q.peekLast();

      if (last == null || candle.t > last.t) {
        q.addLast(candle);
      } else if (candle.t == last.t) {
        q.removeLast();
        q.addLast(candle);
      } else {
        // V80: rare delayed bar callbacks are inserted in timestamp order instead
        // of being appended after newer bars. This keeps /candles monotonic.
        List<Candle> ordered = new ArrayList<>(q);
        boolean replaced = false;
        for (int i = 0; i < ordered.size(); i++) {
          Candle existing = ordered.get(i);
          if (existing.t == candle.t) {
            ordered.set(i, candle);
            replaced = true;
            break;
          }
          if (existing.t > candle.t) {
            ordered.add(i, candle);
            replaced = true;
            break;
          }
        }
        if (!replaced) ordered.add(candle);
        q.clear();
        q.addAll(ordered);
      }

      while (q.size() > 2000) {
        q.removeFirst();
      }
    }

    lastBarAt = System.currentTimeMillis();
  }

  static void startHttp(int port) throws Exception {
    HttpServer server =
        HttpServer.create(
            new InetSocketAddress("0.0.0.0", port),
            0
        );

    server.createContext(
        "/health",
        exchange -> reply(
            exchange,
            200,
            "{"
                + "\"ok\":" + connected + ","
                + "\"source\":\"Dukascopy JForex DEMO\","
                + "\"pairs\":" + PAIRS.size() + ","
                + "\"last_bar_at\":" + lastBarAt + ","
                + "\"last_tick_at\":" + lastTickAt + ","
                + "\"tick_pairs\":" + liveTicks.size() + ","
                + "\"error\":" + json(lastError)
                + "}"
        )
    );

    server.createContext("/pairs", Main::pairs);
    server.createContext("/candles", Main::candles);
    server.createContext("/tick", Main::tick);

    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
  }

  static void pairs(HttpExchange exchange) throws IOException {
    StringBuilder out = new StringBuilder(
        "{\"source\":\"Dukascopy JForex\",\"pairs\":["
    );

    int i = 0;
    for (String pair : PAIRS.keySet()) {
      if (i++ > 0) out.append(',');
      out.append(json(pair));
    }

    out.append("]}");
    reply(exchange, 200, out.toString());
  }

  static void candles(HttpExchange exchange) throws IOException {
    Map<String, String> q =
        query(exchange.getRequestURI().getRawQuery());

    String pair =
        normalizePair(q.getOrDefault("pair", "EUR/USD"));

    int count = 500;

    try {
      count =
          Math.max(
              25,
              Math.min(
                  1500,
                  Integer.parseInt(
                      q.getOrDefault("count", "500")
                  )
              )
          );
    } catch (Exception ignored) {}

    Deque<Candle> data = candles.get(pair);

    if (data == null) {
      reply(
          exchange,
          404,
          "{\"error\":\"unsupported pair\",\"pair\":"
              + json(pair) + "}"
      );
      return;
    }

    List<Candle> list;

    synchronized (data) {
      list = new ArrayList<>(data);
    }

    if (list.size() > count) {
      list =
          list.subList(
              list.size() - count,
              list.size()
          );
    }

    StringBuilder out =
        new StringBuilder(
            "{\"pair\":"
                + json(pair)
                + ",\"source\":\"Dukascopy JForex BID\""
                + ",\"count\":"
                + list.size()
                + ",\"candles\":["
        );

    for (int i = 0; i < list.size(); i++) {
      if (i > 0) out.append(',');
      out.append(list.get(i).json());
    }

    out.append("]}");

    reply(exchange, 200, out.toString());
  }

  static void tick(HttpExchange exchange) throws IOException {
    Map<String, String> q = query(exchange.getRequestURI().getRawQuery());
    String pair = normalizePair(q.getOrDefault("pair", "EUR/USD"));

    if (!PAIRS.containsKey(pair)) {
      reply(exchange, 404, "{\"error\":\"unsupported pair\",\"pair\":" + json(pair) + "}");
      return;
    }

    TickState state = liveTicks.get(pair);
    if (state == null) {
      reply(exchange, 503, "{\"error\":\"live tick not available yet\",\"pair\":" + json(pair) + ",\"connected\":" + connected + "}");
      return;
    }

    long ageMs = Math.max(0L, System.currentTimeMillis() - state.t);
    String body = String.format(
        Locale.US,
        "{\"pair\":%s,\"source\":\"Dukascopy JForex BID TICK\",\"connected\":%s,\"time\":%d,\"bid\":%.8f,\"ask\":%.8f,\"age_ms\":%d,\"forming_candle\":{\"t\":%d,\"o\":%.8f,\"h\":%.8f,\"l\":%.8f,\"c\":%.8f}}",
        json(pair),
        connected ? "true" : "false",
        state.t / 1000L,
        state.bid,
        state.ask,
        ageMs,
        state.minuteStart / 1000L,
        state.o, state.h, state.l, state.c
    );
    reply(exchange, 200, body);
  }

  static Map<String, String> query(String q) {
    Map<String, String> map = new HashMap<>();

    if (q != null) {
      for (String part : q.split("&")) {
        String[] a = part.split("=", 2);

        if (a.length == 2) {
          map.put(
              a[0],
              java.net.URLDecoder.decode(
                  a[1],
                  StandardCharsets.UTF_8
              )
          );
        }
      }
    }

    return map;
  }

  static void reply(
      HttpExchange exchange,
      int code,
      String body
  ) throws IOException {

    byte[] bytes =
        body.getBytes(StandardCharsets.UTF_8);

    exchange
        .getResponseHeaders()
        .set(
            "Content-Type",
            "application/json; charset=utf-8"
        );

    exchange.sendResponseHeaders(code, bytes.length);

    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  static String json(String s) {
    if (s == null) return "null";

    return "\""
        + s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
        + "\"";
  }

  static class TickState {
    final long t;
    final long minuteStart;
    final double bid;
    final double ask;
    final double o;
    final double h;
    final double l;
    final double c;

    TickState(
        long t, long minuteStart, double bid, double ask,
        double o, double h, double l, double c
    ) {
      this.t = t;
      this.minuteStart = minuteStart;
      this.bid = bid;
      this.ask = ask;
      this.o = o;
      this.h = h;
      this.l = l;
      this.c = c;
    }
  }

  static class Candle {
    long t;
    double o;
    double h;
    double l;
    double c;

    Candle(
        long t,
        double o,
        double h,
        double l,
        double c
    ) {
      this.t = t;
      this.o = o;
      this.h = h;
      this.l = l;
      this.c = c;
    }

    String json() {
      return String.format(
          Locale.US,
          "{\"t\":%d,\"o\":%.8f,\"h\":%.8f,\"l\":%.8f,\"c\":%.8f}",
          t / 1000,
          o,
          h,
          l,
          c
      );
    }
  }
}
