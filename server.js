require("dotenv").config();

const express = require("express");
const mqtt = require("mqtt");
const path = require("path");

const app = express();

app.use(express.json());

app.use(
  express.static(
    path.join(
      __dirname,
      "public"
    )
  )
);

const PORT =
  Number(
    process.env.PORT ||
    3000
  );

// ============================================================
// MQTT CONFIGURATION
// ============================================================

const MQTT_HOST =
  process.env.MQTT_HOST;

const MQTT_PORT =
  Number(
    process.env.MQTT_PORT ||
    8883
  );

const MQTT_USERNAME =
  process.env.MQTT_USERNAME;

const MQTT_PASSWORD =
  process.env.MQTT_PASSWORD;

const MQTT_TLS_REJECT_UNAUTHORIZED =
  String(
    process.env
      .MQTT_TLS_REJECT_UNAUTHORIZED ||
    "true"
  ).toLowerCase() !==
  "false";

// ============================================================
// DASHBOARD ADMIN PASSWORD
// ============================================================

const DASHBOARD_ADMIN_PASSWORD =
  process.env
    .DASHBOARD_ADMIN_PASSWORD ||
  "";

// ============================================================
// OTA DEVICE SECRETS
// ============================================================

let otaDeviceSecrets = {};

try {

  if (
    process.env
      .OTA_DEVICE_SECRETS_JSON
  ) {

    otaDeviceSecrets =
      JSON.parse(
        process.env
          .OTA_DEVICE_SECRETS_JSON
      );
  }

}
catch (error) {

  console.error(
    "OTA_DEVICE_SECRETS_JSON is invalid JSON"
  );

  otaDeviceSecrets = {};
}

// ============================================================
// MAIN DATA STORAGE
// ============================================================

const stations =
  new Map();

const ignoredStations =
  new Set();

const logs =
  [];

const clients =
  new Set();

const MAX_LOGS =
  300;

// ============================================================
// HISTORY STORAGE
// ============================================================
//
// Each station gets its own array:
//
// SKY_TEST
// [
//   {
//     timestamp,
//     temperature,
//     wind_speed,
//     rain_adc,
//     rain
//   }
// ]
//
// History is kept for 24 hours.
//
// NOTE:
// This is RAM storage.
// Restarting Node / Render will clear history.
// Later we can move this to a database.
//
// ============================================================

const stationHistory =
  new Map();

const HISTORY_RETENTION_MS =
  24 *
  60 *
  60 *
  1000;

// Extra safety so a broken station cannot fill RAM forever.

const MAX_HISTORY_POINTS_PER_STATION =
  10000;

// ============================================================
// SENSOR LOG CHANGE THRESHOLDS
// ============================================================

const TEMP_CHANGE_THRESHOLD =
  0.2;

const WIND_CHANGE_THRESHOLD =
  0.2;

// ============================================================
// ADMIN PASSWORD CHECK
// ============================================================

function checkAdminPassword(
  password
) {

  if (
    !DASHBOARD_ADMIN_PASSWORD
  ) {

    return false;
  }

  return (
    String(
      password ||
      ""
    ) ===
    DASHBOARD_ADMIN_PASSWORD
  );
}

// ============================================================
// SSE BROADCAST
// ============================================================

function broadcast(
  type,
  data
) {

  const packet =
    `event: ${type}\n` +
    `data: ${JSON.stringify(
      data
    )}\n\n`;

  for (
    const client
    of clients
  ) {

    try {

      client.write(
        packet
      );

    }
    catch (_) {

      clients.delete(
        client
      );
    }
  }
}

// ============================================================
// LOG SYSTEM
// ============================================================

function addLog(
  level,
  source,
  message
) {

  const entry = {

    time:
      new Date()
        .toISOString(),

    level,

    source,

    message
  };

  logs.push(
    entry
  );

  while (
    logs.length >
    MAX_LOGS
  ) {

    logs.shift();
  }

  console.log(
    `[${entry.time}] [${level}] [${source}] ${message}`
  );

  broadcast(
    "log",
    entry
  );
}

// ============================================================
// GENERAL HELPERS
// ============================================================

function numberChanged(
  oldValue,
  newValue,
  threshold
) {

  if (
    oldValue === null ||
    oldValue === undefined ||
    Number.isNaN(
      Number(
        oldValue
      )
    )
  ) {

    return true;
  }

  return (
    Math.abs(
      Number(
        newValue
      ) -
      Number(
        oldValue
      )
    ) >=
    threshold
  );
}


function formatNumber(
  value,
  decimals = 1
) {

  if (
    value === null ||
    value === undefined ||
    Number.isNaN(
      Number(
        value
      )
    )
  ) {

    return "--";
  }

  return Number(
    value
  ).toFixed(
    decimals
  );
}


function safeNumber(
  value
) {

  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {

    return null;
  }

  const number =
    Number(
      value
    );

  if (
    !Number.isFinite(
      number
    )
  ) {

    return null;
  }

  return number;
}

// ============================================================
// HISTORY HELPERS
// ============================================================

function getStationHistoryArray(
  stationID
) {

  if (
    !stationHistory.has(
      stationID
    )
  ) {

    stationHistory.set(
      stationID,
      []
    );
  }

  return stationHistory.get(
    stationID
  );
}


// ============================================================
// REMOVE OLD HISTORY
// ============================================================

function trimStationHistory(
  stationID
) {

  const history =
    getStationHistoryArray(
      stationID
    );

  const oldestAllowed =
    Date.now() -
    HISTORY_RETENTION_MS;

  while (
    history.length >
      0 &&
    history[0].timestamp_ms <
      oldestAllowed
  ) {

    history.shift();
  }

  while (
    history.length >
    MAX_HISTORY_POINTS_PER_STATION
  ) {

    history.shift();
  }
}


// ============================================================
// ADD HISTORY POINT
// ============================================================

function addHistoryPoint(
  stationID,
  data
) {

  const history =
    getStationHistoryArray(
      stationID
    );

  const now =
    Date.now();

  const point = {

    timestamp:
      new Date(
        now
      ).toISOString(),

    timestamp_ms:
      now,

    temperature:
      safeNumber(
        data.temperature
      ),

    wind_speed:
      safeNumber(
        data.wind_speed
      ),

    rain_adc:
      safeNumber(
        data.rain_adc
      ),

    rain:
      data.rain !==
        undefined &&
      data.rain !==
        null
        ?
        String(
          data.rain
        )
        :
        null
  };

  history.push(
    point
  );

  trimStationHistory(
    stationID
  );

  broadcast(
    "history_point",
    {
      station_id:
        stationID,

      ...point
    }
  );
}


// ============================================================
// HISTORY RANGE
// ============================================================

function rangeToMilliseconds(
  range
) {

  switch (
    String(
      range ||
      "1h"
    ).toLowerCase()
  ) {

    case "1h":

      return (
        1 *
        60 *
        60 *
        1000
      );


    case "6h":

      return (
        6 *
        60 *
        60 *
        1000
      );


    case "24h":

      return (
        24 *
        60 *
        60 *
        1000
      );


    default:

      return (
        1 *
        60 *
        60 *
        1000
      );
  }
}


// ============================================================
// CALCULATE STATISTICS
// ============================================================

function calculateStats(
  values
) {

  const valid =
    values.filter(
      value =>
        value !== null &&
        value !== undefined &&
        Number.isFinite(
          Number(
            value
          )
        )
    )
    .map(
      value =>
        Number(
          value
        )
    );

  if (
    valid.length ===
    0
  ) {

    return {

      current:
        null,

      average:
        null,

      minimum:
        null,

      maximum:
        null,

      samples:
        0
    };
  }

  const total =
    valid.reduce(
      (
        sum,
        value
      ) =>
        sum +
        value,
      0
    );

  return {

    current:
      valid[
        valid.length -
        1
      ],

    average:
      total /
      valid.length,

    minimum:
      Math.min(
        ...valid
      ),

    maximum:
      Math.max(
        ...valid
      ),

    samples:
      valid.length
  };
}


// ============================================================
// MQTT CLIENT
// ============================================================

if (
  !MQTT_HOST
) {

  console.error(
    "MQTT_HOST is missing."
  );
}

const mqttClient =
  mqtt.connect(
    `mqtts://${MQTT_HOST}:${MQTT_PORT}`,
    {

      username:
        MQTT_USERNAME,

      password:
        MQTT_PASSWORD,

      reconnectPeriod:
        5000,

      connectTimeout:
        20000,

      clean:
        true,

      keepalive:
        30,

      clientId:
        "weather-dashboard-" +
        Math.random()
          .toString(16)
          .slice(
            2,
            10
          ),

      rejectUnauthorized:
        MQTT_TLS_REJECT_UNAUTHORIZED
    }
  );

// ============================================================
// HEALTH
// ============================================================

app.get(
  "/health",
  (
    req,
    res
  ) => {

    res.json({

      ok:
        true,

      mqtt_connected:
        mqttClient.connected,

      stations:
        stations.size,

      deleted_stations:
        ignoredStations.size,

      history_stations:
        stationHistory.size,

      uptime_seconds:
        Math.floor(
          process.uptime()
        )
    });
  }
);

// ============================================================
// GET ALL STATIONS
// ============================================================

app.get(
  "/api/stations",
  (
    req,
    res
  ) => {

    const result =
      [
        ...stations.values()
      ]
      .sort(
        (
          a,
          b
        ) =>
          String(
            a.station_id
          ).localeCompare(
            String(
              b.station_id
            )
          )
      );

    res.json(
      result
    );
  }
);

// ============================================================
// GET ONE STATION
// ============================================================

app.get(
  "/api/stations/:stationID",
  (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    const station =
      stations.get(
        stationID
      );

    if (
      !station
    ) {

      return res
        .status(
          404
        )
        .json({

          ok:
            false,

          error:
            "Station not found"
        });
    }

    res.json(
      station
    );
  }
);

// ============================================================
// GET STATION HISTORY
// ============================================================
//
// Examples:
//
// /api/stations/SKY_TEST/history
//
// /api/stations/SKY_TEST/history?range=1h
//
// /api/stations/SKY_TEST/history?range=6h
//
// /api/stations/SKY_TEST/history?range=24h
//
// ============================================================

app.get(
  "/api/stations/:stationID/history",
  (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    const requestedRange =
      String(
        req.query.range ||
        "1h"
      ).toLowerCase();

    const allowedRanges = [
      "1h",
      "6h",
      "24h"
    ];

    const range =
      allowedRanges.includes(
        requestedRange
      )
        ?
        requestedRange
        :
        "1h";

    const rangeMilliseconds =
      rangeToMilliseconds(
        range
      );

    const cutoff =
      Date.now() -
      rangeMilliseconds;

    trimStationHistory(
      stationID
    );

    const history =
      getStationHistoryArray(
        stationID
      );

    const points =
      history
        .filter(
          point =>
            point.timestamp_ms >=
            cutoff
        )
        .map(
          point => ({
            timestamp:
              point.timestamp,

            temperature:
              point.temperature,

            wind_speed:
              point.wind_speed,

            rain_adc:
              point.rain_adc,

            rain:
              point.rain
          })
        );

    const temperatureStats =
      calculateStats(
        points.map(
          point =>
            point.temperature
        )
      );

    const windStats =
      calculateStats(
        points.map(
          point =>
            point.wind_speed
        )
      );

    const rainAdcStats =
      calculateStats(
        points.map(
          point =>
            point.rain_adc
        )
      );

    res.json({

      station_id:
        stationID,

      range:
        range,

      from:
        new Date(
          cutoff
        ).toISOString(),

      to:
        new Date()
          .toISOString(),

      samples:
        points.length,

      stats: {

        temperature:
          temperatureStats,

        wind:
          windStats,

        rain_adc:
          rainAdcStats
      },

      points:
        points
    });
  }
);

// ============================================================
// GET DELETED STATIONS
// ============================================================

app.get(
  "/api/deleted-stations",
  (
    req,
    res
  ) => {

    const deleted =
      [
        ...ignoredStations
      ]
      .sort(
        (
          a,
          b
        ) =>
          String(
            a
          ).localeCompare(
            String(
              b
            )
          )
      );

    res.json(
      deleted
    );
  }
);

// ============================================================
// LOG API
// ============================================================

app.get(
  "/api/logs",
  (
    req,
    res
  ) => {

    let limit =
      Number(
        req.query.limit ||
        100
      );

    if (
      !Number.isFinite(
        limit
      )
    ) {

      limit =
        100;
    }

    limit =
      Math.max(
        1,
        Math.min(
          limit,
          MAX_LOGS
        )
      );

    res.json(
      logs.slice(
        -limit
      )
    );
  }
);

// ============================================================
// SSE EVENTS
// ============================================================

app.get(
  "/events",
  (
    req,
    res
  ) => {

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

    res.flushHeaders();

    clients.add(
      res
    );

    res.write(
      `event: connected\n` +
      `data: ${JSON.stringify({
        ok:
          true,

        time:
          new Date()
            .toISOString()
      })}\n\n`
    );

    const keepAlive =
      setInterval(
        () => {

          try {

            res.write(
              ": keepalive\n\n"
            );

          }
          catch (_) {}
        },
        25000
      );

    req.on(
      "close",
      () => {

        clearInterval(
          keepAlive
        );

        clients.delete(
          res
        );
      }
    );
  }
);

// ============================================================
// MQTT PUBLISH HELPER
// ============================================================

function publishMQTT(
  topic,
  payload
) {

  return new Promise(
    (
      resolve,
      reject
    ) => {

      if (
        !mqttClient.connected
      ) {

        reject(
          new Error(
            "MQTT broker is not connected"
          )
        );

        return;
      }

      const payloadText =
        typeof payload ===
          "string"
          ?
          payload
          :
          JSON.stringify(
            payload
          );

      mqttClient.publish(
        topic,
        payloadText,
        {

          qos:
            0,

          retain:
            false
        },
        error => {

          if (
            error
          ) {

            reject(
              error
            );

            return;
          }

          resolve();
        }
      );
    }
  );
}

// ============================================================
// REQUEST STATUS
// ============================================================

app.post(
  "/api/stations/:stationID/status",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    try {

      await publishMQTT(
        `weather/${stationID}/command`,
        "status"
      );

      res.json({
        ok:
          true
      });

    }
    catch (
      error
    ) {

      res
        .status(
          500
        )
        .json({

          ok:
            false,

          error:
            error.message
        });
    }
  }
);

// ============================================================
// REMOTE REBOOT
// ============================================================

app.post(
  "/api/stations/:stationID/reboot",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    const adminPassword =
      req.body
        .admin_password;

    if (
      !checkAdminPassword(
        adminPassword
      )
    ) {

      return res
        .status(
          401
        )
        .json({

          ok:
            false,

          error:
            "Incorrect admin password"
        });
    }

    try {

      await publishMQTT(
        `weather/${stationID}/command`,
        {
          command:
            "reboot"
        }
      );

      addLog(
        "INFO",
        stationID,
        "Remote reboot requested"
      );

      res.json({

        ok:
          true,

        message:
          "Reboot command sent"
      });

    }
    catch (
      error
    ) {

      addLog(
        "ERROR",
        stationID,
        `Reboot failed: ${error.message}`
      );

      res
        .status(
          500
        )
        .json({

          ok:
            false,

          error:
            error.message
        });
    }
  }
);

// ============================================================
// REMOTE CONFIGURE
// ============================================================

app.post(
  "/api/stations/:stationID/config",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    const adminPassword =
      req.body
        .admin_password;

    if (
      !checkAdminPassword(
        adminPassword
      )
    ) {

      return res
        .status(
          401
        )
        .json({

          ok:
            false,

          error:
            "Incorrect admin password"
        });
    }

    const command = {

      command:
        "config",

      station:
        stationID
    };


    if (
      typeof req.body
        .station_name ===
      "string"
    ) {

      const value =
        req.body
          .station_name
          .trim();

      if (
        value
      ) {

        command.station_name =
          value;
      }
    }


    if (
      typeof req.body
        .location ===
      "string"
    ) {

      command.location =
        req.body
          .location
          .trim();
    }


    if (
      typeof req.body
        .wifi_ssid ===
      "string"
    ) {

      const wifiSSID =
        req.body
          .wifi_ssid
          .trim();

      if (
        wifiSSID
      ) {

        command.wifi_ssid =
          wifiSSID;
      }
    }


    if (
      typeof req.body
        .wifi_password ===
      "string" &&
      req.body
        .wifi_password
        .length >
        0
    ) {

      command.wifi_password =
        req.body
          .wifi_password;
    }

    try {

      await publishMQTT(
        `weather/${stationID}/command`,
        command
      );

      addLog(
        "INFO",
        stationID,
        "Remote configuration sent"
      );

      res.json({

        ok:
          true,

        message:
          "Configuration command sent"
      });

    }
    catch (
      error
    ) {

      addLog(
        "ERROR",
        stationID,
        `Configuration failed: ${error.message}`
      );

      res
        .status(
          500
        )
        .json({

          ok:
            false,

          error:
            error.message
        });
    }
  }
);

// ============================================================
// OTA UPDATE
// ============================================================

app.post(
  "/api/stations/:stationID/ota",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    const adminPassword =
      req.body
        .admin_password;

    const firmwareURL =
      String(
        req.body
          .firmware_url ||
        ""
      ).trim();

    const version =
      String(
        req.body
          .version ||
        ""
      ).trim();

    if (
      !checkAdminPassword(
        adminPassword
      )
    ) {

      return res
        .status(
          401
        )
        .json({

          ok:
            false,

          error:
            "Incorrect admin password"
        });
    }


    if (
      !firmwareURL.startsWith(
        "https://"
      )
    ) {

      return res
        .status(
          400
        )
        .json({

          ok:
            false,

          error:
            "Firmware URL must use HTTPS"
        });
    }


    if (
      !version
    ) {

      return res
        .status(
          400
        )
        .json({

          ok:
            false,

          error:
            "Firmware version is required"
        });
    }


    const deviceSecret =
      otaDeviceSecrets[
        stationID
      ];

    if (
      !deviceSecret
    ) {

      return res
        .status(
          400
        )
        .json({

          ok:
            false,

          error:
            `No OTA secret configured for ${stationID}`
        });
    }


    const otaCommand = {

      command:
        "ota",

      station:
        stationID,

      secret:
        deviceSecret,

      url:
        firmwareURL,

      version:
        version
    };


    try {

      await publishMQTT(
        `weather/${stationID}/ota`,
        otaCommand
      );

      addLog(
        "OTA",
        stationID,
        `OTA command sent → ${version}`
      );

      res.json({

        ok:
          true,

        message:
          "OTA command sent",

        station:
          stationID,

        version:
          version
      });

    }
    catch (
      error
    ) {

      addLog(
        "ERROR",
        stationID,
        `OTA command failed: ${error.message}`
      );

      res
        .status(
          500
        )
        .json({

          ok:
            false,

          error:
            error.message
        });
    }
  }
);

// ============================================================
// DELETE STATION
// ============================================================

app.delete(
  "/api/stations/:stationID",
  (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    const adminPassword =
      req.body
        .admin_password;

    if (
      !checkAdminPassword(
        adminPassword
      )
    ) {

      return res
        .status(
          401
        )
        .json({

          ok:
            false,

          error:
            "Incorrect admin password"
        });
    }


    stations.delete(
      stationID
    );

    ignoredStations.add(
      stationID
    );


    addLog(
      "WARN",
      stationID,
      "Station removed from dashboard"
    );


    broadcast(
      "station_deleted",
      {

        station_id:
          stationID
      }
    );


    broadcast(
      "deleted_stations",
      [
        ...ignoredStations
      ]
    );


    res.json({

      ok:
        true,

      message:
        `${stationID} deleted`
    });
  }
);

// ============================================================
// RESTORE STATION
// ============================================================

app.post(
  "/api/stations/:stationID/restore",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    const adminPassword =
      req.body
        .admin_password;

    if (
      !checkAdminPassword(
        adminPassword
      )
    ) {

      return res
        .status(
          401
        )
        .json({

          ok:
            false,

          error:
            "Incorrect admin password"
        });
    }


    if (
      !ignoredStations.has(
        stationID
      )
    ) {

      return res
        .status(
          404
        )
        .json({

          ok:
            false,

          error:
            "Station is not in deleted list"
        });
    }


    ignoredStations.delete(
      stationID
    );


    addLog(
      "INFO",
      stationID,
      "Station restored"
    );


    broadcast(
      "station_restored",
      {

        station_id:
          stationID
      }
    );


    broadcast(
      "deleted_stations",
      [
        ...ignoredStations
      ]
    );


    try {

      if (
        mqttClient.connected
      ) {

        await publishMQTT(
          `weather/${stationID}/command`,
          "status"
        );
      }

    }
    catch (
      error
    ) {

      console.log(
        `Could not request status from ${stationID}: ${error.message}`
      );
    }


    res.json({

      ok:
        true,

      message:
        `${stationID} restored`
    });
  }
);

// ============================================================
// MQTT CONNECT
// ============================================================

mqttClient.on(
  "connect",
  () => {

    addLog(
      "SUCCESS",
      "SERVER",
      "MQTT broker connected"
    );

    const topics = [

      "weather/+/telemetry",

      "weather/+/status",

      "weather/+/ota/status"
    ];


    mqttClient.subscribe(
      topics,
      {

        qos:
          0
      },
      error => {

        if (
          error
        ) {

          addLog(
            "ERROR",
            "SERVER",
            `MQTT subscription failed: ${error.message}`
          );

          return;
        }


        addLog(
          "SUCCESS",
          "SERVER",
          "Listening for weather stations"
        );
      }
    );
  }
);

// ============================================================
// MQTT RECONNECT
// ============================================================

mqttClient.on(
  "reconnect",
  () => {

    addLog(
      "WARN",
      "SERVER",
      "MQTT reconnecting..."
    );
  }
);

// ============================================================
// MQTT OFFLINE
// ============================================================

mqttClient.on(
  "offline",
  () => {

    addLog(
      "ERROR",
      "SERVER",
      "MQTT broker offline"
    );
  }
);

// ============================================================
// MQTT ERROR
// ============================================================

mqttClient.on(
  "error",
  error => {

    addLog(
      "ERROR",
      "SERVER",
      error.message
    );
  }
);

// ============================================================
// MQTT MESSAGE
// ============================================================

mqttClient.on(
  "message",
  (
    topic,
    buffer
  ) => {

    try {

      const parts =
        topic.split(
          "/"
        );


      if (
        parts.length <
          3 ||
        parts[0] !==
          "weather"
      ) {

        return;
      }


      const stationID =
        parts[1];


      if (
        ignoredStations.has(
          stationID
        )
      ) {

        return;
      }


      const messageType =
        parts
          .slice(
            2
          )
          .join(
            "/"
          );


      const data =
        JSON.parse(
          buffer.toString()
        );


      const oldStation =
        stations.get(
          stationID
        );


      const station =
        oldStation
          ?
          {
            ...oldStation
          }
          :
          {

            station_id:
              stationID,

            station_name:
              stationID,

            location:
              "",

            online:
              false,

            last_seen:
              null
          };


      // ======================================================
      // COMMON DATA
      // ======================================================

      station.station_id =
        stationID;


      station.station_name =
        data.station_name ||
        station.station_name ||
        stationID;


      station.location =
        data.location ||
        station.location ||
        "";


      station.last_seen =
        new Date()
          .toISOString();


      const displayName =
        station.station_name ||
        stationID;


      // ======================================================
      // TELEMETRY
      // ======================================================

      if (
        messageType ===
        "telemetry"
      ) {

        // ----------------------------------------------------
        // SAVE HISTORY FIRST
        // ----------------------------------------------------

        addHistoryPoint(
          stationID,
          data
        );


        const wasOnline =
          station.online ===
          true;


        station.online =
          true;


        if (
          !wasOnline
        ) {

          addLog(
            "ONLINE",
            displayName,
            "Station online"
          );
        }


        // ----------------------------------------------------
        // TEMPERATURE
        // ----------------------------------------------------

        if (
          data.temperature !==
            undefined &&
          data.temperature !==
            null
        ) {

          const oldTemperature =
            station.temperature;


          const newTemperature =
            Number(
              data.temperature
            );


          if (
            oldTemperature !==
              undefined &&
            oldTemperature !==
              null &&
            numberChanged(
              oldTemperature,
              newTemperature,
              TEMP_CHANGE_THRESHOLD
            )
          ) {

            addLog(
              "TEMP",
              displayName,
              `${formatNumber(
                oldTemperature,
                1
              )} °C → ${formatNumber(
                newTemperature,
                1
              )} °C`
            );
          }


          station.temperature =
            newTemperature;
        }


        // ----------------------------------------------------
        // PRESSURE
        // ----------------------------------------------------

        if (
          data.pressure !==
            undefined &&
          data.pressure !==
            null
        ) {

          station.pressure =
            Number(
              data.pressure
            );
        }


        // ----------------------------------------------------
        // RAIN STATUS
        // ----------------------------------------------------

        if (
          data.rain !==
            undefined &&
          data.rain !==
            null
        ) {

          const oldRain =
            station.rain;


          const newRain =
            String(
              data.rain
            );


          if (
            oldRain !==
              undefined &&
            oldRain !==
              null &&
            oldRain !==
              newRain
          ) {

            addLog(
              "RAIN",
              displayName,
              `${oldRain} → ${newRain}`
            );
          }


          station.rain =
            newRain;
        }


        // ----------------------------------------------------
        // RAIN ADC
        // ----------------------------------------------------

        if (
          data.rain_adc !==
            undefined &&
          data.rain_adc !==
            null
        ) {

          station.rain_adc =
            Number(
              data.rain_adc
            );
        }


        // ----------------------------------------------------
        // WIND
        // ----------------------------------------------------

        if (
          data.wind_speed !==
            undefined &&
          data.wind_speed !==
            null
        ) {

          const oldWind =
            station.wind_speed;


          const newWind =
            Number(
              data.wind_speed
            );


          if (
            oldWind !==
              undefined &&
            oldWind !==
              null &&
            numberChanged(
              oldWind,
              newWind,
              WIND_CHANGE_THRESHOLD
            )
          ) {

            addLog(
              "WIND",
              displayName,
              `${formatNumber(
                oldWind,
                2
              )} → ${formatNumber(
                newWind,
                2
              )} m/s`
            );
          }


          station.wind_speed =
            newWind;
        }


        // ----------------------------------------------------
        // DEVICE INFO
        // ----------------------------------------------------

        station.firmware =
          data.firmware ||
          station.firmware ||
          null;


        station.network =
          data.network ||
          station.network ||
          null;


        station.ip =
          data.ip ||
          station.ip ||
          null;


        station.mac =
          data.mac ||
          station.mac ||
          null;


        station.wifi_rssi =
          data.wifi_rssi ??
          station.wifi_rssi ??
          null;


        station.link_speed =
          data.link_speed ??
          station.link_speed ??
          null;
      }


      // ======================================================
      // STATUS
      // ======================================================

      else if (
        messageType ===
        "status"
      ) {

        const previousOnline =
          station.online;


        const newOnline =
          typeof data.online ===
            "boolean"
            ?
            data.online
            :
            true;


        station.online =
          newOnline;


        station.firmware =
          data.firmware ||
          station.firmware ||
          null;


        station.network =
          data.network ||
          station.network ||
          null;


        station.ip =
          data.ip ||
          station.ip ||
          null;


        station.mac =
          data.mac ||
          station.mac ||
          null;


        station.wifi_rssi =
          data.wifi_rssi ??
          station.wifi_rssi ??
          null;


        station.link_speed =
          data.link_speed ??
          station.link_speed ??
          null;


        station.ads1115 =
          data.ads1115 ??
          station.ads1115 ??
          null;


        station.bmp280 =
          data.bmp280 ??
          station.bmp280 ??
          null;


        if (
          previousOnline !==
          newOnline
        ) {

          if (
            newOnline
          ) {

            addLog(
              "ONLINE",
              displayName,
              "Station online"
            );

          }
          else {

            addLog(
              "OFFLINE",
              displayName,
              "Station offline"
            );
          }
        }
      }


      // ======================================================
      // OTA STATUS
      // ======================================================

      else if (
        messageType ===
        "ota/status"
      ) {

        const status =
          data.status ||
          "unknown";


        const message =
          data.message ||
          "";


        station.ota_status =
          status;


        station.ota_message =
          message;


        addLog(
          "OTA",
          displayName,
          message
            ?
            `${status}: ${message}`
            :
            status
        );
      }


      // ======================================================
      // STORE STATION
      // ======================================================

      stations.set(
        stationID,
        station
      );


      // ======================================================
      // SEND LIVE UPDATE
      // ======================================================

      broadcast(
        "station",
        station
      );
    }

    catch (
      error
    ) {

      addLog(
        "ERROR",
        "SERVER",
        `MQTT message error: ${error.message}`
      );
    }
  }
);

// ============================================================
// OFFLINE DETECTOR
// ============================================================

setInterval(
  () => {

    const now =
      Date.now();


    for (
      const [
        stationID,
        station
      ]
      of stations
    ) {

      if (
        !station.last_seen
      ) {

        continue;
      }


      const lastSeen =
        new Date(
          station.last_seen
        ).getTime();


      if (
        now -
          lastSeen >
          75000 &&
        station.online
      ) {

        station.online =
          false;


        stations.set(
          stationID,
          station
        );


        addLog(
          "OFFLINE",
          station.station_name ||
          stationID,
          "No data received for 75 seconds"
        );


        broadcast(
          "station",
          station
        );
      }
    }
  },
  15000
);

// ============================================================
// HISTORY CLEANUP
// ============================================================

setInterval(
  () => {

    for (
      const stationID
      of stationHistory.keys()
    ) {

      trimStationHistory(
        stationID
      );
    }

  },
  5 *
  60 *
  1000
);

// ============================================================
// START SERVER
// ============================================================

app.listen(
  PORT,
  "0.0.0.0",
  () => {

    addLog(
      "SUCCESS",
      "SERVER",
      `Dashboard started on port ${PORT}`
    );
  }
);