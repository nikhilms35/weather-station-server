require("dotenv").config();

const express = require("express");
const mqtt = require("mqtt");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

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
    process.env.MQTT_TLS_REJECT_UNAUTHORIZED ||
    "true"
  ).toLowerCase() !== "false";

// ============================================================
// DASHBOARD ADMIN
// ============================================================

const DASHBOARD_ADMIN_PASSWORD =
  process.env.DASHBOARD_ADMIN_PASSWORD ||
  "";

// ============================================================
// SUPABASE - PERMANENT STATION DELETE
// ============================================================

const SUPABASE_URL =
  process.env.SUPABASE_URL ||
  "";

const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  "";

let supabase = null;

if (
  SUPABASE_URL &&
  SUPABASE_SERVICE_ROLE_KEY
) {

  supabase =
    createClient(
      SUPABASE_URL,
      SUPABASE_SERVICE_ROLE_KEY,
      {
        auth: {
          persistSession: false,
          autoRefreshToken: false
        }
      }
    );
}

// ============================================================
// OTA DEVICE SECRETS
// ============================================================

let otaDeviceSecrets = {};

try {

  if (
    process.env.OTA_DEVICE_SECRETS_JSON
  ) {

    otaDeviceSecrets =
      JSON.parse(
        process.env.OTA_DEVICE_SECRETS_JSON
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
// MAIN STORAGE
// ============================================================

const stations =
  new Map();

const ignoredStations =
  new Set();

const permanentlyDeletedStations =
  new Set();

let permanentDeleteListLoaded =
  false;

const logs =
  [];

const clients =
  new Set();

const MAX_LOGS =
  300;

// ============================================================
// HISTORY STORAGE
// ============================================================

const stationHistory =
  new Map();

const HISTORY_RETENTION_MS =
  24 *
  60 *
  60 *
  1000;

const MAX_HISTORY_POINTS_PER_STATION =
  10000;

// ============================================================
// CHANGE THRESHOLDS
// ============================================================

const TEMP_CHANGE_THRESHOLD =
  0.2;

const WIND_CHANGE_THRESHOLD =
  0.2;

// ============================================================
// OFFLINE DETECTION
// ============================================================

// Station is considered offline if no valid station message
// is received for 30 seconds.

const OFFLINE_TIMEOUT_MS =
  30000;

// Check every 5 seconds.

const OFFLINE_CHECK_INTERVAL_MS =
  5000;

// ============================================================
// ADMIN CHECK
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
// PERMANENT DELETE HELPERS
// ============================================================

async function loadPermanentlyDeletedStations() {

  if (
    !supabase
  ) {

    addLog(
      "WARN",
      "SERVER",
      "Supabase permanent-delete storage is not configured"
    );

    return false;
  }

  try {

    const {
      data,
      error
    } =
      await supabase
        .from(
          "deleted_stations"
        )
        .select(
          "station_id"
        );

    if (
      error
    ) {

      throw error;
    }

    permanentlyDeletedStations.clear();

    for (
      const row
      of data || []
    ) {

      const stationID =
        String(
          row.station_id ||
          ""
        ).trim();

      if (
        stationID
      ) {

        permanentlyDeletedStations.add(
          stationID
        );

        ignoredStations.delete(
          stationID
        );

        stations.delete(
          stationID
        );

        stationHistory.delete(
          stationID
        );
      }
    }

    permanentDeleteListLoaded =
      true;

    addLog(
      "SUCCESS",
      "SERVER",
      `Loaded ${permanentlyDeletedStations.size} permanently deleted station(s)`
    );

    return true;
  }

  catch (
    error
  ) {

    permanentDeleteListLoaded =
      false;

    addLog(
      "ERROR",
      "SERVER",
      `Failed to load permanent delete list: ${error.message}`
    );

    return false;
  }
}


function isPermanentlyDeleted(
  stationID
) {

  return permanentlyDeletedStations.has(
    String(
      stationID ||
      ""
    )
  );
}


async function savePermanentDelete(
  stationID
) {

  if (
    !supabase
  ) {

    throw new Error(
      "Supabase permanent-delete storage is not configured"
    );
  }

  const {
    error
  } =
    await supabase
      .from(
        "deleted_stations"
      )
      .upsert(
        {
          station_id:
            stationID,

          deleted_at:
            new Date()
              .toISOString()
        },
        {
          onConflict:
            "station_id"
        }
      );

  if (
    error
  ) {

    throw error;
  }
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
// HELPERS
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
      data.rain !== undefined &&
      data.rain !== null
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
// STATISTICS
// ============================================================

function calculateStats(
  values
) {

  const valid =
    values
      .filter(
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

      permanently_deleted_stations:
        permanentlyDeletedStations.size,

      permanent_delete_list_loaded:
        permanentDeleteListLoaded,

      supabase_configured:
        Boolean(
          supabase
        ),

      history_stations:
        stationHistory.size,

      offline_timeout_seconds:
        OFFLINE_TIMEOUT_MS /
        1000,

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

    if (
      isPermanentlyDeleted(
        stationID
      )
    ) {

      return res
        .status(
          410
        )
        .json({

          ok:
            false,

          error:
            "Station has been permanently deleted"
        });
    }

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
// HISTORY API
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

    res.json({

      station_id:
        stationID,

      range,

      samples:
        points.length,

      stats: {

        temperature:
          calculateStats(
            points.map(
              point =>
                point.temperature
            )
          ),

        wind:
          calculateStats(
            points.map(
              point =>
                point.wind_speed
            )
          ),

        rain_adc:
          calculateStats(
            points.map(
              point =>
                point.rain_adc
            )
          )
      },

      points
    });
  }
);

// ============================================================
// DELETED STATIONS
// ============================================================

app.get(
  "/api/deleted-stations",
  (
    req,
    res
  ) => {

    res.json(
      [
        ...ignoredStations
      ].sort()
    );
  }
);

// ============================================================
// PERMANENTLY DELETED STATIONS
// ============================================================

app.get(
  "/api/permanently-deleted-stations",
  (
    req,
    res
  ) => {

    res.json(
      [
        ...permanentlyDeletedStations
      ].sort()
    );
  }
);


// ============================================================
// PERMANENT DELETE STATION
// ============================================================

app.delete(
  "/api/stations/:stationID/permanent",
  async (
    req,
    res
  ) => {

    const stationID =
      String(
        req.params.stationID ||
        ""
      ).trim();

    if (
      !checkAdminPassword(
        req.body.admin_password
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
      !stationID
    ) {

      return res
        .status(
          400
        )
        .json({

          ok:
            false,

          error:
            "Station ID is required"
        });
    }

    try {

      await savePermanentDelete(
        stationID
      );

      permanentlyDeletedStations.add(
        stationID
      );

      stations.delete(
        stationID
      );

      stationHistory.delete(
        stationID
      );

      ignoredStations.delete(
        stationID
      );

      addLog(
        "WARN",
        stationID,
        "Station permanently deleted"
      );

      broadcast(
        "station_deleted",
        {
          station_id:
            stationID,

          permanent:
            true
        }
      );

      broadcast(
        "deleted_stations",
        [
          ...ignoredStations
        ]
      );

      broadcast(
        "permanently_deleted_stations",
        [
          ...permanentlyDeletedStations
        ]
      );

      return res.json({

        ok:
          true,

        permanent:
          true,

        station_id:
          stationID,

        message:
          `${stationID} permanently deleted`
      });
    }

    catch (
      error
    ) {

      addLog(
        "ERROR",
        stationID,
        `Permanent delete failed: ${error.message}`
      );

      return res
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
// SSE
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
// MQTT PUBLISH
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

    if (
      isPermanentlyDeleted(
        stationID
      )
    ) {

      return res
        .status(
          410
        )
        .json({

          ok:
            false,

          error:
            "Station has already been permanently deleted"
        });
    }

    if (
      isPermanentlyDeleted(
        stationID
      )
    ) {

      return res
        .status(
          410
        )
        .json({

          ok:
            false,

          error:
            "Station has been permanently deleted"
        });
    }

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
// REBOOT
// ============================================================

app.post(
  "/api/stations/:stationID/reboot",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    if (
      isPermanentlyDeleted(
        stationID
      )
    ) {

      return res
        .status(
          410
        )
        .json({

          ok:
            false,

          error:
            "Station has been permanently deleted"
        });
    }

    if (
      !checkAdminPassword(
        req.body.admin_password
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
// CONFIG
// ============================================================

app.post(
  "/api/stations/:stationID/config",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    if (
      isPermanentlyDeleted(
        stationID
      )
    ) {

      return res
        .status(
          410
        )
        .json({

          ok:
            false,

          error:
            "Station has been permanently deleted"
        });
    }

    if (
      !checkAdminPassword(
        req.body.admin_password
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
      typeof req.body.station_name ===
      "string"
    ) {

      const value =
        req.body.station_name.trim();

      if (
        value
      ) {

        command.station_name =
          value;
      }
    }

    if (
      typeof req.body.location ===
      "string"
    ) {

      command.location =
        req.body.location.trim();
    }

    if (
      typeof req.body.wifi_ssid ===
      "string"
    ) {

      const ssid =
        req.body.wifi_ssid.trim();

      if (
        ssid
      ) {

        command.wifi_ssid =
          ssid;
      }
    }

    if (
      typeof req.body.wifi_password ===
      "string" &&
      req.body.wifi_password.length >
      0
    ) {

      command.wifi_password =
        req.body.wifi_password;
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
// OTA
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
      req.body.admin_password;

    const firmwareURL =
      String(
        req.body.firmware_url ||
        ""
      ).trim();

    const version =
      String(
        req.body.version ||
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

    try {

      await publishMQTT(
        `weather/${stationID}/ota`,
        {

          command:
            "ota",

          station:
            stationID,

          secret:
            deviceSecret,

          url:
            firmwareURL,

          version
        }
      );

      addLog(
        "OTA",
        stationID,
        `OTA command sent â†’ ${version}`
      );

      res.json({

        ok:
          true,

        message:
          "OTA command sent"
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
// DELETE
// ============================================================

app.delete(
  "/api/stations/:stationID",
  (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    if (
      !checkAdminPassword(
        req.body.admin_password
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
// RESTORE
// ============================================================

app.post(
  "/api/stations/:stationID/restore",
  async (
    req,
    res
  ) => {

    const stationID =
      req.params.stationID;

    if (
      isPermanentlyDeleted(
        stationID
      )
    ) {

      return res
        .status(
          410
        )
        .json({

          ok:
            false,

          error:
            "Permanently deleted stations cannot be restored from the dashboard"
        });
    }

    if (
      !checkAdminPassword(
        req.body.admin_password
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
    catch (_) {}

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
// MQTT CONNECTION EVENTS
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
        isPermanentlyDeleted(
          stationID
        )
      ) {

        return;
      }

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

      // IMPORTANT:
      // Every valid message from this station updates last_seen.

      station.last_seen =
        new Date()
          .toISOString();

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
              )} Â°C â†’ ${formatNumber(
                newTemperature,
                1
              )} Â°C`
            );
          }

          station.temperature =
            newTemperature;
        }

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
              `${oldRain} â†’ ${newRain}`
            );
          }

          station.rain =
            newRain;
        }

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
              )} â†’ ${formatNumber(
                newWind,
                2
              )} m/s`
            );
          }

          station.wind_speed =
            newWind;
        }

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

          addLog(
            newOnline
              ?
              "ONLINE"
              :
              "OFFLINE",

            displayName,

            newOnline
              ?
              "Station online"
              :
              "Station offline"
          );
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

      stations.set(
        stationID,
        station
      );

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
// FAST OFFLINE DETECTOR
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

      const silenceTime =
        now -
        lastSeen;

      if (
        silenceTime >
          OFFLINE_TIMEOUT_MS &&
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
          "No data received for 30 seconds"
        );

        broadcast(
          "station",
          station
        );
      }
    }
  },
  OFFLINE_CHECK_INTERVAL_MS
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

async function startServer() {

  await loadPermanentlyDeletedStations();

  app.listen(
    PORT,
    "0.0.0.0",
    () => {

      addLog(
        "SUCCESS",
        "SERVER",
        `Dashboard started on port ${PORT}`
      );

      addLog(
        "INFO",
        "SERVER",
        `Offline timeout: ${OFFLINE_TIMEOUT_MS / 1000}s`
      );

      addLog(
        "INFO",
        "SERVER",
        `Permanent deleted stations: ${permanentlyDeletedStations.size}`
      );
    }
  );
}


startServer();