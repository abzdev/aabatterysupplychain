const API_PREFIX = '/api'
const DEFAULT_ACTOR = 'demo-user'

async function readJson(response) {
  const text = await response.text()
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    return { message: text }
  }
}

async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {})
  headers.set('X-Actor', headers.get('X-Actor') || DEFAULT_ACTOR)

  let body = options.body
  if (body && typeof body !== 'string' && !(body instanceof FormData)) {
    headers.set('Content-Type', 'application/json')
    body = JSON.stringify(body)
  }

  const response = await fetch(`${API_PREFIX}${path}`, {
    ...options,
    headers,
    body,
    cache: 'no-store',
  })

  const payload = await readJson(response)
  if (!response.ok) {
    throw new Error(payload?.detail || payload?.message || `Request failed with ${response.status}`)
  }
  return payload
}

export function getEvents(params = {}) {
  const search = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      search.set(key, value)
    }
  })
  const suffix = search.toString() ? `?${search.toString()}` : ''
  return apiFetch(`/events${suffix}`)
}

export function getEvent(eventId) {
  return apiFetch(`/events/${eventId}`)
}

export function analyzeEvent(eventId) {
  return apiFetch(`/agent/analyze/${eventId}`, { method: 'POST' })
}

export function runAgent(payload = {}) {
  return apiFetch('/agent/run', { method: 'POST', body: payload })
}

export function createTransferRequest(payload) {
  return apiFetch('/transfer-requests', { method: 'POST', body: payload })
}

export function approveTransferRequest(requestId) {
  return apiFetch(`/transfer-requests/${requestId}/approve`, { method: 'POST' })
}

export function rejectTransferRequest(requestId, reason) {
  return apiFetch(`/transfer-requests/${requestId}/reject`, {
    method: 'POST',
    body: { reason },
  })
}

export function getApprovalQueue() {
  return apiFetch('/approval-queue')
}

export function getAudit(entityId) {
  return apiFetch(`/audit/${entityId}`)
}

export function getInventoryHealth(params = {}) {
  const search = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      search.set(key, value)
    }
  })
  const suffix = search.toString() ? `?${search.toString()}` : ''
  return apiFetch(`/inventory-health${suffix}`)
}

export function getComparison() {
  return apiFetch('/comparison')
}

export function getLatestAgentRun() {
  return apiFetch('/agent/runs/latest')
}

export function getAgentRun(runId) {
  return apiFetch(`/agent/runs/${runId}`)
}

export function getAgentActivity(params = {}) {
  const search = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      search.set(key, value)
    }
  })
  const suffix = search.toString() ? `?${search.toString()}` : ''
  return apiFetch(`/agent/activity${suffix}`)
}
