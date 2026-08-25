import express from "express";
import cors from "cors";
import mqtt from "mqtt";

// ============================================================
// CONFIGURATION
// ============================================================

const PORT =
  process.env.PORT || 3000;

const MQTT_BROKER =
  process.env.MQTT_BROKER;

const MQTT_USERNAME =
  process.env.MQTT_USERNAME;

const MQTT_PASSWORD =
  process.env.MQTT_PASSWORD;

const OTA_API_TOKEN =
  process.env.OTA_API_TOKEN;

const OTA_DEVICE_SECRET =
  process.env.OTA_DEVICE_SECRET;

// ============================================================
// STATION
// ============================================================

const STATION_ID =
  "GJ-01";

// ============================================================
// MQTT TOPICS
// ============================================================

const TELEMETRY_TOPIC =
  "weather/GJ-01/telemetry";

const STATUS_TOPIC =
  "weather/GJ-01/status";

const COMMAND_TOPIC =
  "weather/GJ-01/command";

const OTA_TOPIC =
  "weather/GJ-01/ota";

// Later WT32 will publish OTA progress here.
const OTA_STATUS_TOPIC =
  "weather/GJ-01/ota/status";

// ============================================================
// DEVICE TIMEOUT
// ============================================================

const OFFLINE_TIMEOUT =
  30000;

// ============================================================
// CHECK ENVIRONMENT
// ============================================================

const requiredEnvironment =
[
  "MQTT_BROKER",
  "MQTT_USERNAME",
  "MQTT_PASSWORD",
  "OTA_API_TOKEN",
  "OTA_DEVICE_SECRET"
];

for (const name of requiredEnvironment)
{
  if (!process.env[name])
  {
    console.error(
      `WARNING: ${name} is missing`
    );
  }
}

// ============================================================
// EXPRESS
// ============================================================

const app =
  express();

app.use(
  cors()
);

app.use(
  express.json()
);

// ============================================================
// CURRENT WEATHER DATA
// ============================================================

let latestWeather =
{
  station:
    STATION_ID,

  temperature:
    null,

  pressure:
    null,

  rain:
    null,

  wind_speed:
    null,

  firmware:
    null,

  counter:
    null,

  uptime:
    null,

  ip:
    null,

  mac:
    null,

  link_speed:
    null,

  online:
    false,

  received_at:
    null,

  mqtt_topic:
    TELEMETRY_TOPIC
};

// ============================================================
// OTA STATE
// ============================================================

let otaState =
{
  status:
    "idle",

  requested_version:
    null,

  firmware_url:
    null,

  requested_at:
    null,

  last_device_message:
    null
};

let lastMessageTime =
  0;

// ============================================================
// SSE CLIENTS
// ============================================================

const sseClients =
  new Set();

function broadcastWeather()
{
  const data =
  {
    weather:
      latestWeather,

    ota:
      otaState
  };

  const payload =
    `data: ${JSON.stringify(data)}\n\n`;

  for (
    const client
    of sseClients
  )
  {
    try
    {
      client.write(
        payload
      );
    }
    catch (error)
    {
      console.error(
        "SSE write error:",
        error.message
      );
    }
  }
}

// ============================================================
// MQTT
// ============================================================

console.log();
console.log(
  "======================================"
);

console.log(
  "WT32 WEATHER STATION BACKEND"
);

console.log(
  "======================================"
);

console.log(
  "Broker:"
);

console.log(
  MQTT_BROKER
);

console.log(
  "MQTT User:"
);

console.log(
  MQTT_USERNAME
);

console.log();

// ============================================================
// MQTT CONNECTION
// ============================================================

const mqttClient =
  mqtt.connect(
    MQTT_BROKER,
    {
      username:
        MQTT_USERNAME,

      password:
        MQTT_PASSWORD,

      clientId:
        "WeatherServer_" +
        Math.random()
          .toString(16)
          .substring(2, 10),

      clean:
        true,

      reconnectPeriod:
        3000,

      connectTimeout:
        10000,

      keepalive:
        30
    }
  );

// ============================================================
// MQTT CONNECTED
// ============================================================

mqttClient.on(
  "connect",
  () =>
  {
    console.log();
    console.log(
      "PRIVATE EMQX MQTT CONNECTED"
    );

    mqttClient.subscribe(
      [
        TELEMETRY_TOPIC,
        STATUS_TOPIC,
        OTA_STATUS_TOPIC
      ],
      {
        qos: 0
      },
      (error) =>
      {
        if (error)
        {
          console.error(
            "MQTT subscription failed:",
            error.message
          );

          return;
        }

        console.log(
          "Subscribed:"
        );

        console.log(
          TELEMETRY_TOPIC
        );

        console.log(
          STATUS_TOPIC
        );

        console.log(
          OTA_STATUS_TOPIC
        );

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

    console.log();
    console.log(
      "MQTT RX:"
    );

    console.log(
      "Topic:",
      topic
    );

    console.log(
      raw
    );

    try
    {
      const data =
        JSON.parse(
          raw
        );

      // ======================================================
      // WEATHER TELEMETRY
      // ======================================================

      if (
        topic ===
        TELEMETRY_TOPIC
      )
      {
        lastMessageTime =
          Date.now();

        latestWeather =
        {
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

          link_speed:
            data.link_speed ??
            latestWeather.link_speed,

          online:
            true,

          received_at:
            new Date()
              .toISOString(),

          mqtt_topic:
            topic
        };

        console.log(
          "Weather data updated."
        );

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
      }

      // ======================================================
      // DEVICE STATUS
      // ======================================================

      if (
        topic ===
        STATUS_TOPIC
      )
      {
        latestWeather =
        {
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
          "Device status updated."
        );
      }

      // ======================================================
      // OTA STATUS
      // ======================================================

      if (
        topic ===
        OTA_STATUS_TOPIC
      )
      {
        otaState =
        {
          ...otaState,

          status:
            data.status ??
            otaState.status,

          last_device_message:
            data,

          updated_at:
            new Date()
              .toISOString()
        };

        console.log(
          "OTA status updated:"
        );

        console.log(
          otaState
        );
      }

      broadcastWeather();
    }
    catch (error)
    {
      console.error(
        "Invalid MQTT JSON:",
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
      "MQTT offline"
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

mqttClient.on(
  "close",
  () =>
  {
    console.log(
      "MQTT connection closed"
    );
  }
);

// ============================================================
// DEVICE OFFLINE CHECK
// ============================================================

setInterval(
  () =>
  {
    if (
      lastMessageTime === 0
    )
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
        "WT32 STATION OFFLINE"
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
    res.json(
      {
        service:
          "WT32 Weather Station Backend",

        status:
          "running",

        station:
          STATION_ID,

        mqtt_connected:
          mqttClient.connected,

        broker:
          "Private EMQX TLS",

        ota:
          "protected"
      }
    );
  }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  "/health",
  (req, res) =>
  {
    res.json(
      {
        ok:
          true,

        mqtt_connected:
          mqttClient.connected,

        station_online:
          latestWeather.online,

        last_message:
          latestWeather.received_at,

        server_time:
          new Date()
            .toISOString()
      }
    );
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
// OTA STATUS API
// ============================================================

app.get(
  "/api/ota/status",
  (req, res) =>
  {
    res.json(
      otaState
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

    const initial =
    {
      weather:
        latestWeather,

      ota:
        otaState
    };

    res.write(
      `data: ${JSON.stringify(initial)}\n\n`
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
// CHECK API TOKEN
// ============================================================

function checkApiToken(
  req,
  res,
  next
)
{
  const authorization =
    req.headers.authorization;

  if (!authorization)
  {
    return res
      .status(401)
      .json(
        {
          ok:
            false,

          error:
            "Authorization header missing"
        }
      );
  }

  const expected =
    `Bearer ${OTA_API_TOKEN}`;

  if (
    authorization !==
    expected
  )
  {
    return res
      .status(403)
      .json(
        {
          ok:
            false,

          error:
            "Invalid OTA API token"
        }
      );
  }

  next();
}

// ============================================================
// OTA COMMAND
// ============================================================
//
// SAFE TEST MODE.
//
// This sends an OTA command to the WT32,
// but your current WT32 firmware will NOT flash.
//
// ============================================================

app.post(
  "/api/ota",
  checkApiToken,

  (req, res) =>
  {
    const version =
      req.body.version;

    const firmwareUrl =
      req.body.url;

    if (!version)
    {
      return res
        .status(400)
        .json(
          {
            ok:
              false,

            error:
              "version is required"
          }
        );
    }

    if (!firmwareUrl)
    {
      return res
        .status(400)
        .json(
          {
            ok:
              false,

            error:
              "url is required"
          }
        );
    }

    if (
      !mqttClient.connected
    )
    {
      return res
        .status(503)
        .json(
          {
            ok:
              false,

            error:
              "MQTT broker is not connected"
          }
        );
    }

    const command =
    {
      command:
        "ota",

      station:
        STATION_ID,

      version:
        version,

      url:
        firmwareUrl,

      secret:
        OTA_DEVICE_SECRET,

      requested_at:
        new Date()
          .toISOString()
    };

    const message =
      JSON.stringify(
        command
      );

    mqttClient.publish(
      OTA_TOPIC,

      message,

      {
        qos:
          1,

        retain:
          false
      },

      (error) =>
      {
        if (error)
        {
          console.error(
            "OTA publish error:",
            error.message
          );

          return res
            .status(500)
            .json(
              {
                ok:
                  false,

                error:
                  error.message
              }
            );
        }

        otaState =
        {
          status:
            "command_sent",

          requested_version:
            version,

          firmware_url:
            firmwareUrl,

          requested_at:
            new Date()
              .toISOString(),

          last_device_message:
            null
        };

        broadcastWeather();

        console.log();
        console.log(
          "OTA COMMAND SENT"
        );

        console.log(
          "Version:",
          version
        );

        console.log(
          "URL:",
          firmwareUrl
        );

        res.json(
          {
            ok:
              true,

            status:
              "command_sent",

            version:
              version,

            topic:
              OTA_TOPIC
          }
        );
      }
    );
  }
);

// ============================================================
// SAFE COMMAND ENDPOINT
// ============================================================

app.post(
  "/api/command",
  checkApiToken,

  (req, res) =>
  {
    const command =
      req.body.command;

    const allowed =
    [
      "ping",
      "status"
    ];

    if (
      !allowed.includes(
        command
      )
    )
    {
      return res
        .status(400)
        .json(
          {
            ok:
              false,

            error:
              "Only ping and status are currently allowed"
          }
        );
    }

    if (
      !mqttClient.connected
    )
    {
      return res
        .status(503)
        .json(
          {
            ok:
              false,

            error:
              "MQTT not connected"
          }
        );
    }

    mqttClient.publish(
      COMMAND_TOPIC,
      command,

      {
        qos:
          0,

        retain:
          false
      }
    );

    res.json(
      {
        ok:
          true,

        command:
          command
      }
    );
  }
);

// ============================================================
// START
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
      "Endpoints:"
    );

    console.log(
      "/api/weather"
    );

    console.log(
      "/api/events"
    );

    console.log(
      "/api/ota/status"
    );

    console.log(
      "/api/ota"
    );

    console.log();
  }
);