// Typed wrapper around the Lindley backend API (proxied to /api in dev).

export type Job = 'vision' | 'assemble' | 'chat' | 'embed'
export const JOBS: Job[] = ['vision', 'assemble', 'chat', 'embed']

export interface Health {
  status: string
  version: string
  ocr_engine: string
  // The AI connection doing each job, or null when none is set up.
  ai: Record<Job, string | null>
}

// What the UI shows beside a page (lindley.browse.state).
export type PageState = 'reading' | 'failed' | 'checked' | 'ai_reading' | 'needs_ai' | 'ai_failed' | 'review' | 'ok'
export type Where = 'inbox' | 'document' | 'aside'

export interface Page {
  id: number
  file: string
  page_index: number
  origin: 'watched' | 'added'
  imported_at: string
  scanned_at: string | null
  where: Where
  document_id: number | null
  position: number | null
  state: PageState
  source: 'tesseract' | 'vision' | 'user' | null
  confidence: number | null
  why: string | null
  script: 'handwritten' | 'printed' | 'typed' | 'mixed' | 'none' | null
  blank: boolean
  dpi: number | null
  size: [number, number] | null
  color_mode: string | null
  turned: number
  /** A mirror image (the back of a carbon copy), turned round to be read and shown */
  mirrored: boolean
  /** Lindley found it was a mirror image */
  mirror_found: boolean
  image: string
  duplicate_of?: { id: number; file: string | null } | null
}

export interface PageFull extends Page {
  text: string
  engine_model: string | null
  unsure: [number, number][]
  readings: number
  document?: { id: number; name: string; suggested: boolean; status: string; page_number: number }
}

export interface DocBase {
  id: number
  name: string
  suggested: boolean
  origin: 'lindley' | 'user'
  status: 'progress' | 'complete'
  folder_id: number | null
  doc_type: string | null
  doc_date: string | null
  date_source: string | null
  confidence: number | null
}

export interface DocSummary extends DocBase {
  pages: number
  to_review: number
  needs_ai: number
  ready: boolean
  exported_at: string | null
  first_page: number | null
}

export interface Check { key: string; label: string; done: boolean; detail: string }

export interface DocFull extends DocBase {
  reasons: string[]
  summary: string | null
  pages: Page[]
  progress: { ready: boolean; checks: Check[] }
  export: { file_name: string; exported_at: string } | null
  inbox_hints: number[]
}

export interface Folder { id: number; parent_id: number | null; name: string; position: number }

export interface Counts {
  inbox: number
  aside: number
  review: number
  duplicates: number
  needs_ai: number
  reading: number
  failed: number
}

/** AI work in hand: reading hard pages, or sorting pages; `asked`: a person sent it. */
export interface AiWorking { kind: 'read' | 'sort'; done: number; of: number; asked: boolean; connection: string | null }
/** What came of AI work a person asked for; ids grow, so a newer one is higher. */
export interface AiFinished { id: number; kind: 'read' | 'sort'; message: string; ok: boolean }

export interface Overview {
  version: string
  counts: Counts
  documents: DocSummary[]
  folders: Folder[]
  ai: { working: AiWorking[]; waiting: { kind: 'read' | 'sort'; of: number }[]; finished: AiFinished[] }
}

export interface Candidate {
  document?: number
  pages?: number[]
  name: string
  at: 'start' | 'end'
  confidence: number
  reasons: string[]
}

export interface Suggestion {
  id: number
  kind: 'add_to_document' | 'group_pages' | 'set_aside'
  page_id: number
  document_id: number | null
  document_name: string | null
  confidence: number
  reasons: string[]
  payload: { pages?: number[]; name?: string; type?: string; date?: string; at?: string; candidates?: Candidate[]; checked_by_ai?: boolean }
  /** A group the AI checked, sure enough to be offered for one-click accept (assembler.offer_at) */
  offer: boolean
}

export interface ReviewGroup {
  document: { id: number; name: string; suggested: boolean; folder_id: number | null } | null
  pages: Page[]
}

export interface NeedsAiConnection {
  name: string
  label: string
  where: 'local' | 'cloud'
  allow: 'ask' | 'auto'
  automatic_left: number | null
}

export interface ReadItem {
  page_id: number
  file: string
  confidence: number | null
  failed: boolean
  why: string | null
  document_id: number | null
  document_name: string | null
  position: number | null
  sending: boolean // on its way to the AI, or being read now
}

export interface Proposal {
  pages: number[]
  name: string
  confidence: number
  reasons: string[]
  question?: 'place'
  candidates?: Candidate[]
}

export interface SortItem { id: number; since: string; pages: { id: number; file: string | null }[]; proposal: Proposal[]; sending: boolean }

/** AI work queued: it's done in the background, and the overview says when. */
export interface Sent { queued: number; already: number; connection: string | null }

export interface NeedsAi {
  count: number
  read: { connection: NeedsAiConnection | null; pages: ReadItem[] }
  sort: { connection: NeedsAiConnection | null; items: SortItem[] }
}

export interface DupCopy {
  page_id: number
  file_name: string
  imported_at: string
  dpi: number | null
  width: number | null
  height: number | null
  color_mode: string | null
  file_size: number | null
  confidence: number | null
  corrected: boolean
  where: Where
  document_id: number | null
  document_name: string | null
  position: number | null
  document_touched: boolean
  image: string
  excerpt?: string
  text?: string
  segments?: [string, boolean][]
}

export interface DupSet {
  id: number
  kind: 'same_page' | 'similar'
  score: number
  reasons: string[]
  suggested: number
  why: string[]
  copies: DupCopy[]
}

export interface DupDocPair { documents: [number, number]; names: [string, string]; sets: number[]; extra: Record<string, number[]> }
export interface Duplicates { count: number; sets: DupSet[]; documents: DupDocPair[] }

export interface SearchResult {
  page_id: number
  file: string
  where: Where
  document_id: number | null
  document_name: string | null
  page_number: number | null
  image: string
  snippet: [string, boolean][]
}

// settings.json, as GET /api/settings gives it
export interface ProviderConfig {
  type: string
  label?: string | null
  base_url?: string | null
  model?: string | null
  api_key_env?: string | null
  allow: 'ask' | 'auto'
  daily_limit: number | null
  monthly_limit: number | null
  per_minute: number | null
  at_once: number
  timeout_s?: number
}

export interface Settings {
  watch_folders: string[]
  processing_dir: string
  quarantine_dir: string
  library_dir: string
  db_path: string
  move_files: boolean
  add_mode: 'ask' | 'copy' | 'move'
  ocr: {
    engine: 'hybrid' | 'tesseract' | 'vision'
    tesseract_path: string | null
    languages: string[]
    confidence_threshold: number
    review_below: number
    vision_max_side: number
    workers: number | null
  }
  ai: {
    providers: Record<string, ProviderConfig>
    jobs: Record<Job, { connection: string | null; model: string | null }>
  }
  assembler: Record<string, unknown>
}

export interface Connector {
  id: string
  label: string
  short: string
  where: 'local' | 'cloud'
  company: string
  jobs: Job[]
  default_models: Partial<Record<Job, string>>
  default_base_url: string | null
  needs_key: boolean
  key_url: string | null
}

export interface AiCalls {
  providers: Record<
    string,
    {
      allow: 'ask' | 'auto'
      daily_limit: number | null
      monthly_limit: number | null
      automatic_today: number
      oked_today: number
      automatic_month: number
      oked_month: number
      automatic_left: number | null
      // What the calls cost, estimated from the tokens they used (US dollars); 0 for an AI
      // whose price Lindley doesn't know, such as one on this computer
      spent_today: number
      spent_month: number
      key_hint: string | null
    }
  >
}

// What a change returns: `undo` is the batch POST /api/undo/{batch} takes back.
export interface Change { undo: number | null; document_id?: number | null; folder_id?: number | null; removed?: number[] }

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const form = init?.body instanceof FormData
  let res: Response
  try {
    res = await fetch(`/api${path}`, {
      ...init,
      headers: form ? init?.headers : { 'Content-Type': 'application/json', ...init?.headers },
    })
  } catch {
    throw new ApiError(0, 'Lindley’s backend isn’t answering. Is it running?')
  }
  if (!res.ok) {
    let message = `${res.status} ${res.statusText}`
    try {
      const body = await res.json()
      const d = body.detail
      if (typeof d === 'string') message = d
      else if (Array.isArray(d)) message = d.map((x) => (typeof x === 'string' ? x : x.msg)).join('. ')
    } catch {
      // not JSON: keep the status
    }
    throw new ApiError(res.status, message)
  }
  return res.json() as Promise<T>
}

const get = <T>(path: string) => request<T>(path)
const send = <T>(method: string, path: string, body?: unknown) =>
  request<T>(path, { method, body: body === undefined ? undefined : JSON.stringify(body) })

// A page image at a size: thumbnails ask for less.
export function imageAt(url: string, maxSide: number): string {
  return `${url}${url.includes('?') ? '&' : '?'}max_side=${maxSide}`
}

export const api = {
  health: () => get<Health>('/health'),
  overview: () => get<Overview>('/overview'),
  inbox: () => get<{ pages: Page[] }>('/inbox'),
  aside: () => get<{ pages: Page[] }>('/aside'),
  review: () => get<{ review_below: number; count: number; groups: ReviewGroup[] }>('/review'),
  document: (id: number) => get<DocFull>(`/documents/${id}`),
  page: (id: number) => get<PageFull>(`/pages/${id}`),
  suggestions: () => get<{ suggestions: Suggestion[] }>('/suggestions'),
  needsAi: () => get<NeedsAi>('/needs-ai'),
  duplicates: () => get<Duplicates>('/duplicates'),
  duplicateSet: (id: number) => get<DupSet>(`/duplicates/${id}`),
  search: (q: string) => get<{ query: string; results: SearchResult[] }>(`/search?q=${encodeURIComponent(q)}`),
  settings: () => get<Settings>('/settings'),
  connectors: () => get<Connector[]>('/connectors'),
  aiCalls: () => get<AiCalls>('/settings/ai-calls'),
  setupNeeded: () => get<{ needed: boolean }>('/setup'),

  movePages: (page_ids: number[], to: Where, document_id?: number) =>
    send<Change>('POST', '/pages/move', { page_ids, to, document_id }),
  rotate: (page_ids: number[], degrees: 90 | -90 | 180) => send<Change>('POST', '/pages/rotate', { page_ids, degrees }),
  flip: (page_ids: number[]) => send<Change>('POST', '/pages/flip', { page_ids }),
  checkText: (id: number, text?: string) => send<Change>('PUT', `/pages/${id}/text`, text === undefined ? {} : { text }),
  newDocument: (page_ids: number[], name: string, folder_id: number | null, suggested = false) =>
    send<Change>('POST', '/documents', { page_ids, name, folder_id, suggested }),
  updateDocument: (id: number, changes: { name?: string; doc_type?: string | null; doc_date?: string | null; folder_id?: number | null }) =>
    send<Change>('PATCH', `/documents/${id}`, changes),
  reorder: (id: number, page_ids: number[]) => send<Change>('PUT', `/documents/${id}/order`, { page_ids }),
  exportDocument: (id: number) =>
    send<{ file_name: string; pages: number[]; to_review: number[]; unplaced: number[] }>('POST', `/documents/${id}/export`),
  reopen: (id: number) => send<{ document_id: number }>('POST', `/documents/${id}/reopen`),
  newFolder: (name: string, parent_id: number | null) => send<Change>('POST', '/folders', { name, parent_id }),
  renameFolder: (id: number, name: string) => send<Change>('PATCH', `/folders/${id}`, { name }),
  acceptSuggestion: (id: number) => send<{ document_id: number | null; pages: number[]; undo: number }>('POST', `/suggestions/${id}/accept`),
  acceptOffers: () => send<{ documents: number[]; pages: number[]; undo: number }>('POST', '/suggestions/accept-offers'),
  dismissSuggestion: (id: number) => send<{ ok: boolean }>('POST', `/suggestions/${id}/dismiss`),
  readWithAi: (page_ids?: number[]) => send<Sent>('POST', '/needs-ai/read', { page_ids: page_ids ?? null }),
  sortWithAi: (itemId?: number) => send<Sent>('POST', itemId === undefined ? '/needs-ai/sort' : `/needs-ai/${itemId}/sort`),
  keepCopy: (setId: number, page_id: number) => send<Change & { set_aside: number[] }>('POST', `/duplicates/${setId}/keep`, { page_id }),
  keepDocument: (keep: number, other: number) => send<Change & { set_aside: number[] }>('POST', '/duplicates/keep-document', { keep, other }),
  notDuplicates: (setId: number) => send<Change>('POST', `/duplicates/${setId}/not-duplicates`),
  undo: (batch?: number | null) => send<{ undone: number }>('POST', batch ? `/undo/${batch}` : '/undo'),
  saveSettings: (s: Settings) => send<Settings>('PUT', '/settings', s),
  saveKey: (name: string, key: string) => send<{ key_hint: string }>('PUT', `/connections/${encodeURIComponent(name)}/key`, { key }),
  testConnection: (connection: ProviderConfig, name: string | null, key: string | null) =>
    send<{ ok: boolean; message: string }>('POST', '/connections/test', { connection, name, key }),
  addScans: (files: File[]) => {
    const form = new FormData()
    files.forEach((f) => form.append('files', f, f.name))
    return request<{ added: { file: string; status: string; pages: number; error: string | null }[]; reading: number }>('/scans', {
      method: 'POST',
      body: form,
    })
  },
  chat: (question: string) => send<{ answer: string }>('POST', '/chat', { question }),
}
