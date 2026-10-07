// Typed wrapper around the Lindley backend API (proxied to /api in dev).

export type Job = 'vision' | 'assemble' | 'chat' | 'embed' | 'continues'
export const JOBS: Job[] = ['vision', 'assemble', 'chat', 'embed', 'continues']

export interface Health {
  status: string
  version: string
  ocr_engine: string
  // The AI connection doing each job, or null when none is set up.
  ai: Record<Job, string | null>
  // Quit Lindley is offered: started from the menu or `python -m lindley`, not with --reload
  can_quit: boolean
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
  reading_again: number // pages a person turned, to be read again the way they're turned now
  failed: number
}

/** AI work in hand: reading hard pages, sorting pages, or downloading Lindley's own AI; `asked`: a
 *  person sent it. `done` of `of` counts pages when reading, questions asked of the AI when
 *  sorting, and bytes when downloading (`label` says what); `pages`: how many pages it's about
 *  (0: not known). */
export interface AiWorking {
  kind: 'read' | 'sort' | 'download'
  done: number
  of: number
  pages: number
  asked: boolean
  connection: string | null
  label: string | null
}
/** What came of AI work a person asked for; ids grow, so a newer one is higher. */
export interface AiFinished { id: number; kind: 'read' | 'sort' | 'download'; message: string; ok: boolean }

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
  timeout_s?: number | null
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
    tier: string | null // how much AI this computer runs; null once a person changes a job
    help: string | null // the connection that does what the tier leaves; null: nobody
    local: { models_dir: string | null; server_path: string | null; device: string | null }
  }
  assembler: Record<string, unknown>
  ask: { local_chars: number; cloud_chars: number; history_turns: number }
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

/** How much AI this computer runs (backend providers/tiers.py): the jobs Lindley's own AI does
 *  here, each with its model. */
export interface Tier {
  id: string
  label: string
  needs: string
  does: string
  local: Partial<Record<Job, string>>
  downloads: string[] // the models it needs, each once
}

/** Who does the jobs the tier leaves: nobody, your own AI server, or a cloud AI. */
export type HelpKind = 'none' | 'server' | 'cloud'
export interface Help {
  id: HelpKind
  label: string
  does: string
}

/** Lindley's own AI: its engine and models, and what's downloaded. */
export interface LocalAi {
  folder: string
  free: number | null // bytes free on that disk
  engine: { build: string | null; size: number; ready: boolean }
  models: { id: string; label: string; size: number; memory_gb: number; jobs: Job[]; state: 'ready' | 'downloading' | 'missing' }[]
  downloading: { label: string; done: number; of: number } | null
  running: boolean
  on_processor: boolean // the graphics couldn't load a model, so it runs on the processor alone
}

/** A look at this computer (backend localai/computer.py), the tier it suits, and how long 100
 *  pages would take on each tier, in seconds (handwritten null: it isn't read on this computer). */
export interface Computer {
  processor: string | null
  threads: number
  memory: number | null // bytes
  graphics: { name: string; memory: number }[] // cards with memory of their own, the largest first
  free: number | null // bytes free on the disk Lindley's own AI goes on
  engine: boolean // whether Lindley's own AI runs on this kind of computer
  suggested: string // a tier's id
  why: string
  times: Record<string, { typed: number; handwritten: number | null }>
  measured_on: string // the computer the times were measured on
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
  if (!res.ok) throw await errorOf(res)
  return res.json() as Promise<T>
}

async function errorOf(res: Response): Promise<ApiError> {
  let message = `${res.status} ${res.statusText}`
  try {
    const body = await res.json()
    const d = body.detail
    if (typeof d === 'string') message = d
    else if (Array.isArray(d)) message = d.map((x) => (typeof x === 'string' ? x : x.msg)).join('. ')
  } catch {
    // not JSON: keep the status
  }
  return new ApiError(res.status, message)
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
  tiers: () => get<{ tiers: Tier[]; helps: Help[] }>('/tiers'),
  localAi: () => get<LocalAi>('/local-ai'),
  computer: () => get<Computer>('/local-ai/computer'),
  aiCalls: () => get<AiCalls>('/settings/ai-calls'),
  setupNeeded: () => get<{ needed: boolean }>('/setup'),
  quit: () => send<{ stopping: boolean }>('POST', '/quit'),

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
  downloadModels: (models: string[]) => send<{ queued: string[] }>('POST', '/local-ai/download', { models }),
  cancelDownload: () => send<{ ok: boolean }>('POST', '/local-ai/cancel'),
  removeModel: (id: string) => send<{ removed: string }>('DELETE', `/local-ai/models/${encodeURIComponent(id)}`),
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
  chatStatus: () => get<ChatStatus>('/chat/status'),
  conversations: () => get<{ conversations: ChatSummary[] }>('/chat/conversations'),
  conversation: (id: number) => get<{ id: number; title: string; messages: ChatSaved[] }>(`/chat/conversations/${id}`),
  deleteConversation: (id: number) => send<{ deleted: number }>('DELETE', `/chat/conversations/${id}`),
}

// ---------------------------------------------------------------- Ask Lindley

/** What a person has open, which a question is about unless they say otherwise. */
export interface Scope {
  document_id?: number
  page_id?: number
}

export interface ChatConn {
  name: string
  label: string
  where: 'local' | 'cloud'
  company: string | null
}

/** Whether Ask Lindley can answer: ready; broken (its AI can't be used now: `reason` says why);
 * offer (an AI that could answer isn't chosen for it yet: `offers`); none. */
export interface ChatStatus {
  state: 'ready' | 'broken' | 'offer' | 'none'
  connection: ChatConn | null
  reason: string
  offers: ChatConn[]
}

/** A page sent with a question; the answer cites it as [n]. */
export interface ChatSource {
  n: number
  page_id: number
  document_id: number | null
  page_number: number | null
  label: string
  unsure: boolean // read with low confidence, not checked yet
}

export interface ChatSaved {
  id: number
  role: 'user' | 'assistant'
  text: string
  sources: ChatSource[]
  scope: Scope | null
  status: 'done' | 'stopped' | 'failed'
  created_at: string
}

export interface ChatSummary {
  id: number
  title: string
  created_at: string
  updated_at: string
  questions: number
}

export type AskEvent =
  | { event: 'chat'; data: { chat_id: number; message_id: number } }
  | { event: 'sources'; data: { sources: ChatSource[] } }
  | { event: 'text'; data: { text: string } }
  | { event: 'done'; data: { status: 'done' | 'stopped'; message_id: number } }
  | { event: 'error'; data: { message: string; message_id?: number } }

export interface Question {
  question: string
  chat_id: number | null
  scope: Scope
  looking: string
}

/** Ask Lindley a question. The answer comes as server-sent events (an EventSource can't send a
 * question, so they're read from the response here), each given to `on` as it arrives. Aborting
 * `signal` stops the answer: Lindley keeps what was written so far. */
export async function ask(q: Question, on: (e: AskEvent) => void, signal: AbortSignal): Promise<void> {
  let res: Response
  try {
    res = await fetch('/api/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(q), signal })
  } catch {
    if (signal.aborted) return
    throw new ApiError(0, 'Lindley’s backend isn’t answering. Is it running?')
  }
  if (!res.ok || !res.body) throw await errorOf(res)
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  let buf = ''
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) return
      buf += value.replace(/\r\n?/g, '\n')
      for (let cut = buf.indexOf('\n\n'); cut >= 0; cut = buf.indexOf('\n\n')) {
        const frame = buf.slice(0, cut)
        buf = buf.slice(cut + 2)
        let event = 'message'
        const data: string[] = []
        for (const line of frame.split('\n')) {
          if (line.startsWith('event:')) event = line.slice(6).trim()
          else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''))
        }
        // A frame with no data is the server's keep-alive, while the AI thinks
        if (data.length) on({ event, data: JSON.parse(data.join('\n')) } as AskEvent)
      }
    }
  } catch {
    if (signal.aborted) return
    throw new ApiError(0, 'The answer stopped coming: Lindley’s backend may have stopped.')
  }
}
