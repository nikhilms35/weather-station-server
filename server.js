require("dotenv").config();

const express = require("express");
const mqtt = require("mqtt");
const path = require("path");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = Number(process.env.PORT || 3000);

const stations = new Map();
const logs = [];
const MAX_LOGS = 500;
const clients = new Set();

function broadcast(type, data) {
  const packet =
    `event: ${type}\n` +
    `data: ${JSON.stringify(data)}\n\n`;

  for (const client of clients) {
    try {
      client.write(packet);
    } catch (_) {
      clients.delete(client);
    }
  }
}

function addLog(level, source, message) {
  const entry = {
    time: new Date().toISOString(),
    level,
    source,
    message
  };

  logs.push(entry);

  while (logs.length > MAX_LOGS) {
    logs.shift();
  }

  console.log(
    `[${entry.time}] [${level}] [${source}] ${message}`
  );

  broadcast("log", entry);
}

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    mqtt_connected: mqttClient.connected,
    stations: stations.size,
    uptime_seconds: Math.floor(process.uptime())
  });
});

app.get("/api/stations", (req, res) => {
  res.json(
    [...stations.values()].sort((a, b) =>
      String(a.station_id).localeCompare(
        String(b.station_id)
      )
    )
  );
});

app.get("/api/logs", (req, res) => {
  const limit = Math.min(
    Math.max(Number(req.query.limit || 150), 1),
    MAX_LOGS
  );

  res.json(logs.slice(-limit));
});

app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  clients.add(res);

  res.write(
    `event: connected\n` +
    `data: ${JSON.stringify({
      ok: true,
      time: new Date().toISOString()
    })}\n\n`
  );

  const keepAlive = setInterval(() => {
    try {
      res.write(": keepalive\n\n");
    } catch (_) {}
  }, 25000);

  req.on("close", () => {
    clearInterval(keepAlive);
    clients.delete(res);
  });
});

const MQTT_HOST = process.env.MQTT_HOST;
const MQTT_PORT = Number(process.env.MQTT_PORT || 8883);
const MQTT_USERNAME = process.env.MQTT_USERNAME;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD;

const MQTT_TLS_REJECT_UNAUTHORIZED =
  String(
    process.env.MQTT_TLS_REJECT_UNAUTHORIZED || "true"
  ).toLowerCase() !== "false";

if (!MQTT_HOST) {
  console.error("MQTT_HOST is missing.");
}

const mqttClient = mqtt.connect(
  `mqtts://${MQTT_HOST}:${MQTT_PORT}`,
  {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    reconnectPeriod: 5000,
    connectTimeout: 20000,
    clean: true,
    keepalive: 30,
    clientId:
      "weather-dashboard-" +
      Math.random().toString(16).slice(2, 10),
    rejectUnauthorized:
      MQTT_TLS_REJECT_UNAUTHORIZED
  }
);

mqttClient.on("connect", () => {
  addLog(
    "SUCCESS",
    "MQTT",
    `Connected to ${MQTT_HOST}:${MQTT_PORT}`
  );

  const topics = [
    "weather/+/telemetry",
    "weather/+/status",
    "weather/+/ota/status"
  ];

  mqttClient.subscribe(
    topics,
    { qos: 0 },
    error => {
      if (error) {
        addLog(
          "ERROR",
          "MQTT",
          `Subscribe failed: ${error.message}`
        );
        return;
      }

      addLog(
        "SUCCESS",
        "MQTT",
        "Subscribed to all weather stations"
      );
    }
  );
});

mqttClient.on("reconnect", () => {
  addLog(
    "WARN",
    "MQTT",
    "Reconnecting to broker..."
  );
});

mqttClient.on("offline", () => {
  addLog(
    "WARN",
    "MQTT",
    "MQTT client is offline"
  );
});

mqttClient.on("close", () => {
  addLog(
    "WARN",
    "MQTT",
    "Broker connection closed"
  );
});

mqttClient.on("error", error => {
  addLog(
    "ERROR",
    "MQTT",
    error.message
  );
});

mqttClient.on("message", (topic, buffer) => {
  const raw = buffer.toString();

  addLog(
    "RX",
    topic,
    raw.length > 800
      ? raw.substring(0, 800) + "..."
      : raw
  );

  try {
    const parts = topic.split("/");

    if (
      parts.length < 3 ||
      parts[0] !== "weather"
    ) {
      return;
    }

    const stationID = parts[1];
    const messageType =
      parts.slice(2).join("/");

    const data = JSON.parse(raw);

    const current =
      stations.get(stationID) || {
        station_id: stationID,
        station_name: stationID,
        location: "",
        online: false,
        last_seen: null
      };

    const station = {
      ...current,
      station_id: stationID,
      station_name:
        data.station_name ||
        current.station_name ||
        stationID,
      location:
        data.location ||
        current.location ||
        "",
      last_seen:
        new Date().toISOString()
    };

    if (messageType === "telemetry") {
      station.online = true;

      station.temperature =
        data.temperature ??
        station.temperature ??
        null;

      station.pressure =
        data.pressure ??
        station.pressure ??
        null;

      station.rain =
        data.rain ??
        station.rain ??
        null;

      station.wind_speed =
        data.wind_speed ??
        station.wind_speed ??
        null;

      station.firmware =
        data.firmware ||
        station.firmware ||
        null;

      station.ip =
        data.ip ||
        station.ip ||
        null;

      station.mac =
        data.mac ||
        station.mac ||
        null;

      station.link_speed =
        data.link_speed ??
        station.link_speed ??
        null;

      addLog(
        "INFO",
        stationID,
        `Telemetry | Temp=${data.temperature ?? "--"} C | Pressure=${data.pressure ?? "--"} hPa | Wind=${data.wind_speed ?? "--"} m/s | Rain=${data.rain ?? "--"}`
      );
    }

    else if (messageType === "status") {
      station.online =
        typeof data.online === "boolean"
          ? data.online
          : true;

      station.firmware =
        data.firmware ||
        station.firmware ||
        null;

      station.ip =
        data.ip ||
        station.ip ||
        null;

      station.mac =
        data.mac ||
        station.mac ||
        null;

      station.link_speed =
        data.link_speed ??
        station.link_speed ??
        null;

      addLog(
        "INFO",
        stationID,
        `Status | ${station.online ? "ONLINE" : "OFFLINE"}`
      );
    }

    else if (messageType === "ota/status") {
      station.ota_status =
        data.status || null;

      station.ota_message =
        data.message || null;

      addLog(
        "OTA",
        stationID,
        `OTA | ${data.status ?? "--"} | ${data.message ?? ""}`
      );
    }

    stations.set(
      stationID,
      station
    );

    broadcast(
      "station",
      station
    );
  }

  catch (error) {
    addLog(
      "ERROR",
      "MQTT",
      `Message processing failed: ${error.message}`
    );
  }
});

setInterval(() => {
  const now = Date.now();

  for (
    const [stationID, station]
    of stations
  ) {
    if (!station.last_seen) {
      continue;
    }

    const lastSeen =
      new Date(
        station.last_seen
      ).getTime();

    if (
      now - lastSeen > 75000 &&
      station.online
    ) {
      station.online = false;

      stations.set(
        stationID,
        station
      );

      addLog(
        "WARN",
        stationID,
        "Station marked OFFLINE: no MQTT message for 75 seconds"
      );

      broadcast(
        "station",
        station
      );
    }
  }
}, 15000);

app.listen(
  PORT,
  "0.0.0.0",
  () => {
    addLog(
      "SUCCESS",
      "SERVER",
      `Weather dashboard running on port ${PORT}`
    );
  }
);