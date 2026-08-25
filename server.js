import express from "express";
import cors from "cors";
import mqtt from "mqtt";

// ============================================================
// CONFIG
// ============================================================

const PORT = process.env.PORT || 3000;

const MQTT_BROKER =
  process.env.MQTT_BROKER ||
  "mqtt://test.mosquitto.org:1883";

const TELEMETRY_TOPIC =
  "weather/GJ-01/telemetry";

const STATUS_TOPIC =
  "weather/GJ-01/status";

const STATION_ID =
  "GJ-01";

const OFFLINE_TIMEOUT = 30000;

// ============================================================
// EXPRESS
// ============================================================

const app = express();

app.use(cors());
app.use(express.json());

// ============================================================
// WEATHER STATE
// ============================================================

let latestWeather = {
  station: STATION_ID,

  temperature: null,
  pressure: null,
  rain: null,
  wind_speed: null,

  firmware: null,

  counter: null,
  uptime: null,

  ip: null,
  mac: null,

  link_speed: null,

  online: false,

  received_at: null,

  mqtt_topic: TELEMETRY_TOPIC
};

let lastMessageTime = 0;

// ============================================================
// SSE CLIENTS
// ============================================================

const sseClients = new Set();

function broadcastWeather()
{
  const payload =
    `data: ${JSON.stringify(latestWeather)}\n\n`;

  for (const client of sseClients)
  {
    try
    {
      client.write(payload);
    }
    catch (error)
    {
      console.error(
        "SSE error:",
        error.message
      );
    }
  }
}

// ============================================================
// MQTT
// ============================================================

console.log();
console.log("======================================");
console.log("WEATHER STATION BACKEND");
console.log("======================================");
console.log("Broker:", MQTT_BROKER);
console.log();

const mqttClient =
  mqtt.connect(
    MQTT_BROKER,
    {
      clientId:
        "WeatherServer_" +
        Math.random()
          .toString(16)
          .substring(2, 10),

      clean: true,
      reconnectPeriod: 3000,
      connectTimeout: 10000,
      keepalive: 30
    }
  );

// ============================================================
// MQTT CONNECT
// ============================================================

mqttClient.on(
  "connect",
  () =>
  {
    console.log("MQTT CONNECTED");

    mqttClient.subscribe(
      [
        TELEMETRY_TOPIC,
        STATUS_TOPIC
      ],
      {
        qos: 0
      },
      (error) =>
      {
        if (error)
        {
          console.error(
            "Subscribe failed:",
            error.message
          );

          return;
        }

        console.log("Subscribed:");
        console.log(TELEMETRY_TOPIC);
        console.log(STATUS_TOPIC);
        console.log();
      }
    );
  }
);

// ============================================================
// MQTT MESSAGE
// ============================================================

mqttClient.on(
  "message",
  (topic, buffer) =>
  {
    const raw =
      buffer.toString();

    console.log("MQTT RX:");
    console.log(raw);

    try
    {
      const data =
        JSON.parse(raw);

      // ------------------------------------------------------
      // TELEMETRY
      // ------------------------------------------------------

      if (
        topic ===
        TELEMETRY_TOPIC
      )
      {
        lastMessageTime =
          Date.now();

        latestWeather = {
          ...latestWeather,

          station:
            data.station ??
            latestWeather.station,

          temperature:
            data.temperature ??
            latestWeather.temperature,

          pressure:
            data.pressure ??
            latestWeather.pressure,

          rain:
            data.rain ??
            latestWeather.rain,

          wind_speed:
            data.wind_speed ??
            latestWeather.wind_speed,

          firmware:
            data.firmware ??
            latestWeather.firmware,

          counter:
            data.counter ??
            latestWeather.counter,

          uptime:
            data.uptime ??
            latestWeather.uptime,

          ip:
            data.ip ??
            latestWeather.ip,

          mac:
            data.mac ??
            latestWeather.mac,

          online:
            true,

          received_at:
            new Date().toISOString(),

          mqtt_topic:
            topic
        };

        console.log("Weather data updated.");
        console.log(
          "Temperature:",
          latestWeather.temperature
        );

        console.log(
          "Pressure:",
          latestWeather.pressure
        );

        console.log(
          "Rain:",
          latestWeather.rain
        );

        console.log(
          "Wind:",
          latestWeather.wind_speed
        );

        console.log(
          "Counter:",
          latestWeather.counter
        );

        console.log(
          "Firmware:",
          latestWeather.firmware
        );

        console.log(
          "IP:",
          latestWeather.ip
        );

        console.log();
      }

      // ------------------------------------------------------
      // STATUS
      // ------------------------------------------------------

      if (
        topic ===
        STATUS_TOPIC
      )
      {
        latestWeather = {
          ...latestWeather,

          station:
            data.station ??
            latestWeather.station,

          online:
            data.online ??
            latestWeather.online,

          firmware:
            data.firmware ??
            latestWeather.firmware,

          ip:
            data.ip ??
            latestWeather.ip,

          mac:
            data.mac ??
            latestWeather.mac,

          link_speed:
            data.link_speed ??
            latestWeather.link_speed,

          uptime:
            data.uptime ??
            latestWeather.uptime
        };

        console.log(
          "Status updated."
        );
      }

      broadcastWeather();
    }
    catch (error)
    {
      console.error(
        "Invalid JSON:",
        error.message
      );
    }
  }
);

// ============================================================
// MQTT EVENTS
// ============================================================

mqttClient.on(
  "error",
  (error) =>
  {
    console.error(
      "MQTT error:",
      error.message
    );
  }
);

mqttClient.on(
  "offline",
  () =>
  {
    console.log(
      "MQTT connection offline"
    );
  }
);

mqttClient.on(
  "reconnect",
  () =>
  {
    console.log(
      "MQTT reconnecting..."
    );
  }
);

// ============================================================
// OFFLINE CHECK
// ============================================================

setInterval(
  () =>
  {
    if (lastMessageTime === 0)
    {
      return;
    }

    const age =
      Date.now() -
      lastMessageTime;

    const wasOnline =
      latestWeather.online;

    if (
      age >
      OFFLINE_TIMEOUT
    )
    {
      latestWeather.online =
        false;
    }

    if (
      wasOnline &&
      !latestWeather.online
    )
    {
      console.log(
        "WT32 station OFFLINE"
      );

      broadcastWeather();
    }
  },
  5000
);

// ============================================================
// ROOT
// ============================================================

app.get(
  "/",
  (req, res) =>
  {
    res.json({
      service:
        "WT32 Weather Station Backend",

      status:
        "running",

      station:
        STATION_ID,

      mqtt_connected:
        mqttClient.connected
    });
  }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  "/health",
  (req, res) =>
  {
    res.json({
      ok: true,

      mqtt_connected:
        mqttClient.connected,

      station_online:
        latestWeather.online,

      last_message:
        latestWeather.received_at,

      server_time:
        new Date().toISOString()
    });
  }
);

// ============================================================
// WEATHER API
// ============================================================

app.get(
  "/api/weather",
  (req, res) =>
  {
    res.json(
      latestWeather
    );
  }
);

// ============================================================
// LIVE SSE
// ============================================================

app.get(
  "/api/events",
  (req, res) =>
  {
    res.setHeader(
      "Content-Type",
      "text/event-stream"
    );

    res.setHeader(
      "Cache-Control",
      "no-cache"
    );

    res.setHeader(
      "Connection",
      "keep-alive"
    );

    res.setHeader(
      "Access-Control-Allow-Origin",
      "*"
    );

    res.flushHeaders();

    res.write(
      `data: ${JSON.stringify(latestWeather)}\n\n`
    );

    sseClients.add(
      res
    );

    console.log(
      "Dashboard connected. SSE clients:",
      sseClients.size
    );

    req.on(
      "close",
      () =>
      {
        sseClients.delete(
          res
        );

        console.log(
          "Dashboard disconnected. SSE clients:",
          sseClients.size
        );
      }
    );
  }
);

// ============================================================
// START SERVER
// ============================================================

app.listen(
  PORT,
  () =>
  {
    console.log(
      `HTTP server running on port ${PORT}`
    );

    console.log();

    console.log(
      `Weather API: http://localhost:${PORT}/api/weather`
    );

    console.log(
      `Health:      http://localhost:${PORT}/health`
    );

    console.log(
      `Live SSE:    http://localhost:${PORT}/api/events`
    );

    console.log();
  }
);