// Typed wrapper around the Lindley backend API (proxied to /api in dev).

export interface Health {
  status: string
  version: string
  ocr_engine: string
  chat_provider: string | null
}

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    throw new ApiError(res.status, `${res.status} ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

export const api = {
  health: () => request<Health>('/health'),
}
