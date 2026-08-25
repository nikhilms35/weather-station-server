import express from "express";
import cors from "cors";
import mqtt from "mqtt";

const PORT = process.env.PORT || 3000;

const MQTT_BROKER = process.env.MQTT_BROKER;
const MQTT_USERNAME = process.env.MQTT_USERNAME;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD;

// Protects HTTP OTA requests from your admin client.
// Do not put this in a public GitHub Pages JavaScript file.
const OTA_API_TOKEN = process.env.OTA_API_TOKEN;

// JSON map: station ID -> OTA secret.
// Example:
// {"GJ-01":"secret1","GJ-02":"secret2"}
const OTA_DEVICE_SECRETS_JSON =
  process.env.OTA_DEVICE_SECRETS_JSON || "{}";

let otaSecrets = {};

try {
  otaSecrets = JSON.parse(OTA_DEVICE_SECRETS_JSON);
} catch {
  console.error("OTA_DEVICE_SECRETS_JSON is invalid JSON.");
}

const app = express();

app.use(cors());
app.use(express.json());

const stations = {};
const sseClients = new Set();

function ensureStation(id) {
  if (!stations[id]) {
    stations[id] = {
      station_id: id,
      station_name: id,
      location: "",
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
      ota: {
        status: "idle",
        requested_version: null,
        requested_at: null,
        last_device_message: null
      }
    };
  }
  return stations[id];
}

function broadcast() {
  const payload = `data: ${JSON.stringify({
    stations,
    server_time: new Date().toISOString()
  })}\n\n`;

  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch {}
  }
}

function stationFromTopic(topic) {
  // weather/GJ-01/telemetry
  const p = topic.split("/");
  if (p.length < 3 || p[0] !== "weather")
    return null;
  return p[1];
}

const mqttClient = mqtt.connect(
  MQTT_BROKER,
  {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    clientId:
      "WeatherServer_" +
      Math.random().toString(16).substring(2, 10),
    clean: true,
    reconnectPeriod: 3000,
    connectTimeout: 10000,
    keepalive: 30
  }
);

mqttClient.on("connect", () => {
  console.log("PRIVATE EMQX MQTT CONNECTED");

  mqttClient.subscribe(
    [
      "weather/+/telemetry",
      "weather/+/status",
      "weather/+/ota/status"
    ],
    { qos: 0 },
    (err) => {
      if (err) console.error("Subscribe error:", err.message);
      else console.log("Subscribed to all weather stations.");
    }
  );
});

mqttClient.on("message", (topic, buffer) => {
  const id = stationFromTopic(topic);
  if (!id) return;

  let data;

  try {
    data = JSON.parse(buffer.toString());
  } catch {
    console.error("Invalid JSON on", topic);
    return;
  }

  const st = ensureStation(id);

  if (topic.endsWith("/telemetry")) {
    Object.assign(st, {
      station_id: data.station_id ?? data.station ?? id,
      station_name: data.station_name ?? st.station_name,
      location: data.location ?? st.location,
      temperature: data.temperature ?? st.temperature,
      pressure: data.pressure ?? st.pressure,
      rain: data.rain ?? st.rain,
      wind_speed: data.wind_speed ?? st.wind_speed,
      firmware: data.firmware ?? st.firmware,
      counter: data.counter ?? st.counter,
      uptime: data.uptime ?? st.uptime,
      ip: data.ip ?? st.ip,
      mac: data.mac ?? st.mac,
      link_speed: data.link_speed ?? st.link_speed,
      online: true,
      received_at: new Date().toISOString()
    });
  }

  if (topic.endsWith("/status") && !topic.endsWith("/ota/status")) {
    Object.assign(st, {
      station_id: data.station_id ?? id,
      station_name: data.station_name ?? st.station_name,
      location: data.location ?? st.location,
      firmware: data.firmware ?? st.firmware,
      uptime: data.uptime ?? st.uptime,
      ip: data.ip ?? st.ip,
      mac: data.mac ?? st.mac,
      link_speed: data.link_speed ?? st.link_speed,
      online: data.online ?? st.online
    });
  }

  if (topic.endsWith("/ota/status")) {
    st.ota = {
      ...st.ota,
      status: data.status ?? st.ota.status,
      last_device_message: data,
      updated_at: new Date().toISOString()
    };
  }

  broadcast();
});

mqttClient.on("error", err =>
  console.error("MQTT error:", err.message)
);

setInterval(() => {
  const now = Date.now();
  let changed = false;

  for (const st of Object.values(stations)) {
    if (
      st.online &&
      st.received_at &&
      now - Date.parse(st.received_at) > 30000
    ) {
      st.online = false;
      changed = true;
    }
  }

  if (changed) broadcast();
}, 5000);

function checkToken(req, res, next) {
  if (!OTA_API_TOKEN) {
    return res.status(503).json({
      ok: false,
      error: "OTA_API_TOKEN not configured"
    });
  }

  if (req.headers.authorization !== `Bearer ${OTA_API_TOKEN}`) {
    return res.status(403).json({
      ok: false,
      error: "Invalid API token"
    });
  }

  next();
}

app.get("/", (req, res) => {
  res.json({
    service: "WT32 Multi-Station Weather Backend",
    mqtt_connected: mqttClient.connected,
    station_count: Object.keys(stations).length
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    mqtt_connected: mqttClient.connected,
    station_count: Object.keys(stations).length,
    server_time: new Date().toISOString()
  });
});

app.get("/api/stations", (req, res) => {
  res.json(Object.values(stations));
});

app.get("/api/stations/:id", (req, res) => {
  const st = stations[req.params.id];

  if (!st) {
    return res.status(404).json({
      ok: false,
      error: "Station not found"
    });
  }

  res.json(st);
});

app.get("/api/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.flushHeaders();

  res.write(
    `data: ${JSON.stringify({
      stations,
      server_time: new Date().toISOString()
    })}\n\n`
  );

  sseClients.add(res);

  req.on("close", () => {
    sseClients.delete(res);
  });
});

app.post("/api/ota", checkToken, (req, res) => {
  const { station_id, version, url } = req.body;

  if (!station_id || !version || !url) {
    return res.status(400).json({
      ok: false,
      error: "station_id, version and url are required"
    });
  }

  const deviceSecret = otaSecrets[station_id];

  if (!deviceSecret) {
    return res.status(400).json({
      ok: false,
      error: `No OTA secret configured for ${station_id}`
    });
  }

  if (!mqttClient.connected) {
    return res.status(503).json({
      ok: false,
      error: "MQTT not connected"
    });
  }

  const topic = `weather/${station_id}/ota`;

  const command = {
    command: "ota",
    station: station_id,
    version,
    url,
    secret: deviceSecret,
    requested_at: new Date().toISOString()
  };

  mqttClient.publish(
    topic,
    JSON.stringify(command),
    { qos: 1, retain: false },
    err => {
      if (err) {
        return res.status(500).json({
          ok: false,
          error: err.message
        });
      }

      const st = ensureStation(station_id);

      st.ota = {
        ...st.ota,
        status: "command_sent",
        requested_version: version,
        requested_at: new Date().toISOString()
      };

      broadcast();

      res.json({
        ok: true,
        status: "command_sent",
        station_id,
        version,
        topic
      });
    }
  );
});

app.listen(PORT, () => {
  console.log(`HTTP server running on port ${PORT}`);
});
