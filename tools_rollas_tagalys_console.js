function attachRollasTagalysMonitor(root = window) {
  const TARGET_SUBSTRING = 'api-r3.tagalys.com';
  const TARGET_PATH = '/v1/analytics/events/track';
  const FLAG_KEY = '__tagalysTagMonitor';

  if (!root || typeof root !== 'object') {
    throw new Error('attachRollasTagalysMonitor: invalid root object');
  }
  if (root[FLAG_KEY]) {
    root.console?.warn?.('Tagalys monitor already installed.');
    return root[FLAG_KEY];
  }

  const events = [];
  const taCalls = [];

  function logEntries(label, collection) {
    const list = collection || events;
    if (!list.length) {
      root.console?.info?.(`[Tagalys] ${label} – no entries captured yet.`);
      return;
    }
    list.forEach((entry, idx) => {
      const consoleGroup = root.console?.groupCollapsed || root.console?.log;
      const consoleGroupEnd = root.console?.groupEnd || (() => {});
      consoleGroup.call(root.console, `[Tagalys] ${label}[${idx}]`);
      root.console?.log?.(entry);
      consoleGroupEnd.call(root.console);
    });
  }

  const monitor = {
    events,
    taCalls,
    printEvents() {
      logEntries('events', events);
    },
    printTaCalls() {
      logEntries('taCalls', taCalls);
    },
  };

  root[FLAG_KEY] = monitor;

  function logTaCall(args, note) {
    try {
      taCalls.push({
        timestamp: new Date().toISOString(),
        args: Array.from(args || []),
        note,
      });
      const consoleGroup = root.console?.groupCollapsed || root.console?.log;
      const consoleGroupEnd = root.console?.groupEnd || (() => {});
      consoleGroup.call(root.console, `[Tagalys] ta() captured${note ? ` (${note})` : ''}`);
      root.console?.log?.('Arguments:', args);
      if (args && args.length) {
        try {
          const payload = args[args.length - 1];
          if (payload && typeof payload === 'object') {
            root.console?.log?.('Payload preview:', JSON.stringify(payload, null, 2));
          }
        } catch (err) {
          root.console?.warn?.('Tagalys monitor: failed to pretty-print ta payload', err);
        }
      }
      consoleGroupEnd.call(root.console);
    } catch (err) {
      root.console?.warn?.('Tagalys monitor: unable to log ta() call', err);
    }
  }

  function normaliseRawBody(body) {
    if (body == null) return null;
    if (typeof body === 'string') return body;
    if (body instanceof ArrayBuffer) {
      try {
        return new TextDecoder().decode(body);
      } catch (err) {
        return null;
      }
    }
    if (body instanceof Uint8Array) {
      try {
        return new TextDecoder().decode(body);
      } catch (err) {
        return null;
      }
    }
    if (typeof Blob !== 'undefined' && body instanceof Blob) {
      try {
        return body.text();
      } catch (err) {
        return null;
      }
    }
    if (typeof body === 'object' && typeof body.text === 'function') {
      try {
        return body.text();
      } catch (err) {
        return null;
      }
    }
    if (typeof URLSearchParams !== 'undefined' && body instanceof URLSearchParams) {
      return body.toString();
    }
    if (typeof FormData !== 'undefined' && body instanceof FormData) {
      const obj = {};
      for (const [key, value] of body.entries()) {
        obj[key] = value;
      }
      try {
        return JSON.stringify(obj);
      } catch (err) {
        return null;
      }
    }
    try {
      return JSON.stringify(body);
    } catch (err) {
      return null;
    }
  }

  function extractProductArrays(payload) {
    const arrays = [];
    const seen = new Set();

    const stack = [payload];
    while (stack.length) {
      const current = stack.pop();
      if (!current || typeof current !== 'object') continue;
      if (Array.isArray(current)) {
        if (current.length && typeof current[0] === 'object') {
          if (!seen.has(current)) {
            arrays.push(current);
            seen.add(current);
          }
        }
        for (const entry of current) stack.push(entry);
      } else {
        for (const key of Object.keys(current)) {
          stack.push(current[key]);
        }
      }
    }
    return arrays;
  }

  function logPayload(origin, payload, rawString) {
    const timestamp = new Date().toISOString();
    events.push({ timestamp, origin, payload, raw: rawString });

    const consoleGroup = root.console?.groupCollapsed || root.console?.log;
    const consoleGroupEnd = root.console?.groupEnd || (() => {});
    const consoleTable = root.console?.table || root.console?.log;

    consoleGroup.call(root.console, `[Tagalys] ${origin} @ ${timestamp}`);
    root.console?.log?.('Raw payload:', payload);

    const productArrays = extractProductArrays(payload);
    productArrays.forEach((arr, idx) => {
      const label = arr.length ? Object.keys(arr[0]).slice(0, 6).join(', ') : 'No columns';
      consoleTable.call(root.console, `Products[${idx}] (${arr.length} items) — columns: ${label}`, arr);
    });

    const pretty = JSON.stringify(payload, null, 2);
    root.console?.log?.('Pretty JSON:', pretty);
    consoleGroupEnd.call(root.console);
  }

  function parsePayloadString(raw) {
    const attempts = [];
    if (typeof raw === 'string') {
      attempts.push(raw);
      try {
        attempts.push(decodeURIComponent(raw));
      } catch (err) {
        // ignore
      }
    }

    for (const candidate of attempts) {
      if (!candidate) continue;
      try {
        return JSON.parse(candidate);
      } catch (err) {
        // ignore and continue with other strategies
      }
    }

    if (typeof URLSearchParams !== 'undefined' && typeof raw === 'string' && raw.includes('=')) {
      try {
        const params = new URLSearchParams(raw);
        for (const [key, value] of params.entries()) {
          const candidates = [value];
          try {
            candidates.push(decodeURIComponent(value));
          } catch (err) {
            // ignore decode issues
          }
          for (const cand of candidates) {
            if (!cand) continue;
            try {
              return JSON.parse(cand);
            } catch (err) {
              // ignore
            }
          }
        }
        const obj = {};
        for (const [key, value] of params.entries()) {
          obj[key] = value;
        }
        return obj;
      } catch (err) {
        // ignore and fall through
      }
    }

    return null;
  }

  function processBodyPromise(bodyPromise, origin) {
    if (!bodyPromise) return;
    Promise.resolve(bodyPromise)
      .then(raw => (raw && typeof raw.then === 'function' ? raw : Promise.resolve(raw)))
      .then(value => value)
      .then(raw => {
        if (raw == null) return;
        if (typeof raw !== 'string') {
          raw = normaliseRawBody(raw);
        }
        if (typeof raw !== 'string') return;
        const trimmed = raw.trim();
        if (!trimmed) return;
        const payload = parsePayloadString(trimmed);
        if (!payload) {
          root.console?.warn?.('Tagalys monitor: Unable to parse analytics payload', trimmed.slice(0, 200));
          return;
        }
        logPayload(origin, payload, trimmed);
      })
      .catch(err => {
        root.console?.warn?.('Tagalys monitor: Failed to read analytics payload', err);
      });
  }

  function shouldCapture(url) {
    return typeof url === 'string' && url.includes(TARGET_SUBSTRING) && url.includes(TARGET_PATH);
  }

  if (typeof root.fetch === 'function') {
    const originalFetch = root.fetch.bind(root);
    root.fetch = function patchedFetch(input, init = {}) {
      let url;
      let bodyPromise = null;

      if (typeof Request !== 'undefined' && input instanceof Request) {
        url = input.url;
        if (shouldCapture(url)) {
          try {
            bodyPromise = input.clone().text();
          } catch (err) {
            bodyPromise = null;
          }
        }
      } else {
        url = typeof input === 'string' ? input : (input && input.url);
        if (shouldCapture(url)) {
          bodyPromise = normaliseRawBody(init && init.body);
        }
      }

      const responsePromise = originalFetch(input, init);
      if (shouldCapture(url)) {
        processBodyPromise(bodyPromise, 'fetch');
      }
      return responsePromise;
    };
  }

  if (typeof root.XMLHttpRequest === 'function') {
    const OriginalXHR = root.XMLHttpRequest;
    function WrappedXHR() {
      const xhr = new OriginalXHR();
      let targetUrl = null;
      let bodyData = null;

      const originalOpen = xhr.open;
      xhr.open = function(method, url) {
        targetUrl = url;
        return originalOpen.apply(xhr, arguments);
      };

      const originalSend = xhr.send;
      xhr.send = function(body) {
        if (shouldCapture(targetUrl)) {
          bodyData = normaliseRawBody(body);
          processBodyPromise(bodyData, 'xhr');
        }
        return originalSend.apply(xhr, arguments);
      };

      return xhr;
    }
    WrappedXHR.prototype = OriginalXHR.prototype;
    root.XMLHttpRequest = WrappedXHR;
  }

  if (root.navigator && typeof root.navigator.sendBeacon === 'function') {
    const originalSendBeacon = root.navigator.sendBeacon.bind(root.navigator);
    root.navigator.sendBeacon = function patchedSendBeacon(url, data) {
      const shouldLog = shouldCapture(url);
      if (shouldLog) {
        const body = normaliseRawBody(data);
        processBodyPromise(body, 'beacon');
      }
      return originalSendBeacon(url, data);
    };
  }

  root.console?.info?.('[Tagalys] Analytics monitor ready. Waiting for events…');

  const analyticsObjectName = root.TagalysAnalyticsObject || 'ta';

  function copyFunctionProps(source, target) {
    if (typeof source !== 'function') return;
    const names = Object.getOwnPropertyNames(source);
    names.forEach(name => {
      if (['length', 'name', 'prototype', 'caller', 'arguments'].includes(name)) {
        return;
      }
      try {
        target[name] = source[name];
      } catch (err) {
        // ignore readonly assignments
      }
    });
    try {
      Object.setPrototypeOf(target, Object.getPrototypeOf(source));
    } catch (err) {
      // ignore proto assignment issues
    }
  }

  function monitorTaQueue(queue) {
    if (!Array.isArray(queue)) return;
    queue.forEach((entry, idx) => logTaCall(entry, `queued[${idx}]`));
    const originalPush = queue.push;
    queue.push = function patchedPush() {
      for (let i = 0; i < arguments.length; i += 1) {
        logTaCall(arguments[i], 'queued push');
      }
      return originalPush.apply(this, arguments);
    };
  }

  function wrapTaFunction(fn, note) {
    if (typeof fn !== 'function') return null;
    if (fn.__tagalysWrapped) return fn;

    const wrapped = function patchedTa() {
      logTaCall(arguments, note || 'live call');
      return fn.apply(this, arguments);
    };

    wrapped.__tagalysWrapped = true;
    wrapped.__tagalysOriginal = fn;

    if (Array.isArray(fn.q)) {
      fn.q.forEach((queued, idx) => logTaCall(queued, `queued[${idx}]`));
      wrapped.q = fn.q;
    } else if (Array.isArray(fn.queue)) {
      fn.queue.forEach((queued, idx) => logTaCall(queued, `queue[${idx}]`));
      wrapped.queue = fn.queue;
    }

    copyFunctionProps(fn, wrapped);
    return wrapped;
  }

  function installTaWrapper(reason) {
    const candidate = root[analyticsObjectName];
    if (Array.isArray(candidate)) {
      monitorTaQueue(candidate);
      return false;
    }
    if (typeof candidate !== 'function') {
      return false;
    }
    if (candidate.__tagalysWrapped) {
      return true;
    }
    const wrapped = wrapTaFunction(candidate, reason || 'live call');
    if (!wrapped) return false;
    root[analyticsObjectName] = wrapped;
    root.console?.info?.(`[Tagalys] Wrapped ${analyticsObjectName} analytics function (${reason || 'initial wrap'}).`);
    return true;
  }

  if (Array.isArray(root[analyticsObjectName])) {
    monitorTaQueue(root[analyticsObjectName]);
  }

  if (!installTaWrapper('initial state')) {
    const poller = typeof root.setInterval === 'function' ? root.setInterval(() => {
      if (installTaWrapper('polling attach')) {
        if (typeof root.clearInterval === 'function') {
          root.clearInterval(poller);
        }
      }
    }, 500) : null;
    if (poller && typeof monitor === 'object') {
      monitor.stopTaPolling = () => {
        if (typeof root.clearInterval === 'function') {
          root.clearInterval(poller);
        }
      };
    }
  }

  return monitor;
}

if (typeof module === 'object' && module.exports) {
  module.exports = attachRollasTagalysMonitor;
}

if (typeof window !== 'undefined') {
  const monitor = attachRollasTagalysMonitor(window);
  if (monitor) {
    window.console?.info?.('[Tagalys] Current events:', monitor.events);
    window.console?.info?.('[Tagalys] Current ta() calls:', monitor.taCalls);
  }
}
