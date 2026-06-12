async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  let body = null;
  try {
    body = await response.json();
  } catch {
    body = null;
  }

  if (!response.ok) {
    const message = body?.error || body?.message || `Request failed: ${response.status}`;
    throw new Error(message);
  }

  return body;
}

export async function listDlq({ limit = 100, offset = 0 } = {}) {
  return request(`/api/dlq?limit=${encodeURIComponent(limit)}&offset=${encodeURIComponent(offset)}`);
}

export async function getDlq(eventId) {
  return request(`/api/dlq/${encodeURIComponent(eventId)}`);
}

export async function replayDlq(eventId, payload) {
  return request(`/api/dlq/${encodeURIComponent(eventId)}/replay`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getHealth() {
  return request("/healthz");
}