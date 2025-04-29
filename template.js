const JSON = require('JSON');
const getAllEventData = require('getAllEventData');
const getTimestampMillis = require('getTimestampMillis');
const getRequestHeader = require('getRequestHeader');
const getGoogleAuth = require('getGoogleAuth');
const sendHttpRequest = require('sendHttpRequest');
const getType = require('getType');
const logToConsole = require('logToConsole');
const getContainerVersion = require('getContainerVersion');
const Math = require('Math');
const Object = require('Object');
const makeInteger = require('makeInteger');
const encodeUriComponent = require('encodeUriComponent');

// —––––––––––––––––– CONSTANTS –––––––––––––––––—
const MS_PER_DAY = 86400000;
const MS_PER_HOUR = 3600000;
const MS_PER_MINUTE = 60000;

// Container & logging
const containerVersion = getContainerVersion();
const isDebug = containerVersion.debugMode;
const traceId = getRequestHeader('trace-id');
function determinateIsLoggingEnabled() {
  if (!data.logType) return isDebug;
  if (data.logType === 'no') return false;
  if (data.logType === 'debug') return isDebug;
  return data.logType === 'always';
}
const isLoggingEnabled = determinateIsLoggingEnabled();

// —––––––––––––––––– HELPERS –––––––––––––––––—
function pad(num, width) {
  let s = '' + num;
  while (s.length < width) {
    s = '0' + s;
  }
  return s;
}

/**
 * Build a Firestore‐valid ISO timestamp:
 *   ms since epoch → "YYYY-MM-DDTHH:mm:ss.SSSZ"
 */
function buildNumericIso(ts) {
  // time‐of‐day
  let msec = ts % MS_PER_DAY;
  const hour = Math.floor(msec / MS_PER_HOUR);
	msec = msec - hour * MS_PER_HOUR;
  const minute = Math.floor(msec / MS_PER_MINUTE);
	msec = msec - minute * MS_PER_MINUTE;
  const second = Math.floor(msec / 1000);
  const mill = msec - second * 1000;

  // days since 1970-01-01
  let days = Math.floor(ts / MS_PER_DAY);

  // subtract years
  let year = 1970;
  while (true) {
    const isLeap = (year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0));
    const daysInYear = isLeap ? 366 : 365;
    if (days >= daysInYear) {
      days = days - daysInYear;
      year = year + 1;
    } else {
      break;
    }
  }

  // subtract months
  const monthLengths = [
    31,
    ((year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)) ? 29 : 28),
    31,30,31,30,31,31,30,31,30,31
  ];
  let monthIndex = 0;
  while (days >= monthLengths[monthIndex]) {
    days = days - monthLengths[monthIndex];
    monthIndex = monthIndex + 1;
  }

  const month = monthIndex + 1;
  const day   = days + 1;

  return (
    pad(year,4) + '-' + pad(month,2) + '-' + pad(day,2) +
    'T' +
    pad(hour,2)   + ':' + pad(minute,2) + ':' +
    pad(second,2) + '.' + pad(mill,3) + 'Z'
  );
}

/**
 * Wrap any JS value into Firestore REST JSON:
 *  - null       → { nullValue: null }
 *  - string     → { stringValue: ... }
 *  - number     → { doubleValue: ... }
 *  - boolean    → { booleanValue: ... }
 *  - array      → { arrayValue: { values: [...] } }
 *  - object     → { mapValue:   { fields: {...} } }
 */
function wrapValue(v) {
  if (v === null || v === undefined) {
    return { nullValue: null };
  }
  const t = getType(v);
  if (t === 'string') { return { stringValue:  v };}
  if (t === 'number') { return { doubleValue:  v };}
  if (t === 'boolean') { return { booleanValue: v };}
  if (t === 'array') {
    const vals = [];
    for (let i = 0; i < v.length; i = i + 1) {
      vals.push(wrapValue(v[i]));
    }
    return { arrayValue: { values: vals } };
  }
  if (t === 'object') {
    const flds = {};
    const keys = Object.keys(v);
    for (let i = 0; i < keys.length; i = i + 1) {
      const k = keys[i];
      flds[k] = wrapValue(v[k]);
    }
    return { mapValue: { fields: flds } };
  }
  return { nullValue: null };
}

// —––––––––––––––––– BUILD FIRESTORE FIELDS –––––––––––––––––—
const fields = {};

// 1) TTL as a true Firestore Timestamp
if (data.addTimeToLive) {
  const ttlDays  = makeInteger(data.timeToLiveDays);
  const expireMs = getTimestampMillis() + ttlDays * MS_PER_DAY;
  const isoTs = buildNumericIso(expireMs);

  fields[data.timeToLiveFieldName] = {
    timestampValue: isoTs
  };
}

// 2) Raw event timestamp
if (data.addTimestamp) {
  const now = getTimestampMillis();
  fields[data.timestampFieldName] = {
    integerValue: now.toString()
  };
}

// 3) Custom key/value list
if (data.customDataList) {
  data.customDataList.forEach(function(item) {
    const name = item.name;
    const val = item.value;
    if (data.skipNilValues && (val === null || val === undefined)) {
      return;
    }
    fields[name] = wrapValue(val);
  });
}

// 4) Full event payload
if (data.addEventData) {
  const all = getAllEventData();
  const keys = Object.keys(all);
  for (let i = 0; i < keys.length; i = i + 1) {
    const key = keys[i];
    if (fields[key] !== undefined) {
      continue;
    }
    fields[key] = wrapValue(all[key]);
  }
}

// —––––––––––––––––– FIRESTORE REST CALL –––––––––––––––––—
const payload = { fields: fields };
const projectId = data.gcpProjectId;
const rawPath = data.firebasePath;  // e.g. "myCollection" or "myCollection/12345"
const baseUrl   =
  'https://firestore.googleapis.com/v1/projects/' +
  projectId +
  '/databases/(default)/documents/' +
  rawPath;

let url, method;

// if user gave a full path (collection + ID)…
if (rawPath.indexOf('/') > -1) {
  // always PATCH to upsert
  url    = baseUrl;
  method = 'PATCH';

  if (data.firebaseMerge) {
    const fieldNames = Object.keys(fields);
    for (let i = 0; i < fieldNames.length; i = i + 1) {
      const fp = fieldNames[i];
      url = url + (i === 0 ? '?' : '&') + 
            'updateMask.fieldPaths=' + encodeUriComponent(fp);
    }
  }
}
// otherwise, no ID → POST to collection for an auto-generated ID
else {
  url    = baseUrl;
  method = 'POST';
}

const auth = getGoogleAuth({ scopes: ['https://www.googleapis.com/auth/datastore'] });

sendHttpRequest(
  url,
  {
    method: method,
    timeout: 500,
    headers: { 'Content-Type': 'application/json' },
    authorization: auth
  },
  JSON.stringify({ fields: fields })
).then(
  function(res) {
    if (isLoggingEnabled) {
      // parse response body (assumed valid JSON)
      const body = JSON.parse(res.body || '{}');
      let docId = '';
      if (body.name) {
        const parts = body.name.split('/');
        docId = parts[parts.length - 1];
      }
      logToConsole(
        JSON.stringify({
          Name: 'Firestore',
          Type: 'Message',
          TraceId: traceId,
          EventName: 'Write',
          DocumentId: docId,
          DocumentInput: fields
        })
      );
    }
    data.gtmOnSuccess();
  },
  function(err) {
    logToConsole('Firestore ' + method + ' failed: ' + err);
    data.gtmOnFailure();
  }
);
function determinateIsLoggingEnabled() {
  if (!data.logType) {
    return isDebug;
  }

  if (data.logType === 'no') {
    return false;
  }

  if (data.logType === 'debug') {
    return isDebug;
  }

  return data.logType === 'always';
}