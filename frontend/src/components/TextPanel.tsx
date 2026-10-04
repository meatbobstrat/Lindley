// What Lindley read from a page, beside its scan: editable, with the words it wasn't sure of
// marked. The text box is left to the browser while you type; `textRef` reads it back.

import type { ReactNode, RefObject } from 'react'
import type { PageFull } from '../api/client'
import { kindOf, plural, readLong } from '../lib/words'
import { Icon, Mark } from '../ui/icons'

function marked(text: string, unsure: [number, number][]): ReactNode[] {
  const out: ReactNode[] = []
  let at = 0
  ;[...unsure]
    .sort((a, b) => a[0] - b[0])
    .forEach(([s, e], i) => {
      if (s < at || e > text.length) return
      if (s > at) out.push(text.slice(at, s))
      out.push(
        <mark key={i} data-tip="Lindley wasn’t sure of this word. Check it against the scan.">
          {text.slice(s, e)}
        </mark>,
      )
      at = e
    })
  out.push(text.slice(at))
  return out
}

export function TextPanel({
  page,
  textRef,
  label,
  pager,
  where,
  readonly,
  actions,
}: {
  page: PageFull
  textRef: RefObject<HTMLDivElement | null>
  label: string
  pager?: ReactNode
  where?: ReactNode
  readonly?: boolean
  actions?: ReactNode
}) {
  const unsure = page.state === 'checked' ? [] : page.unsure
  let state: ReactNode = null
  if (page.state === 'needs_ai' || page.state === 'ai_failed')
    state = (
      <div className="rv-state flag">
        <Mark />
        <div>
          <b>This scan needs an AI to read it.</b> Tesseract read it with only {page.confidence}% confidence, so this text is a rough guess.
          {page.state === 'ai_failed' && ` The AI was asked to read it and ${page.why || 'didn’t manage'}, so it waits for you to try again.`}{' '}
          Once the AI has read it, check its text here. If you check it yourself first, the AI isn’t needed.
        </div>
      </div>
    )
  else if (page.state === 'review')
    state = (
      <div className="rv-state flag">
        <Icon name="flag" />
        <div>
          <b>Needs your review.</b> Lindley read this page with {page.confidence ?? 'low'}% confidence
          {unsure.length ? ` and wasn’t sure of ${plural(unsure.length, 'word')}` : ''}. Compare the text with the scan and fix anything that’s wrong.
        </div>
      </div>
    )
  else if (page.state === 'checked')
    state = (
      <div className="rv-state mine">
        <Icon name="check" />
        <div>You checked this page. Your text is used for search and chat.</div>
      </div>
    )
  else if (page.state === 'reading')
    state = (
      <div className="rv-state mine">
        <Icon name="eye" />
        <div>Lindley is still reading this scan. Its text will appear here when it’s done.</div>
      </div>
    )

  return (
    <>
      <div className="side-h">
        <strong>Transcription</strong>
        {pager}
      </div>
      {where}
      <div className="engine">
        {kindOf(page)}. {readLong(page)}
        {page.engine_model && page.source !== 'user' ? ` (${page.engine_model})` : ''}.{' '}
        <span className="mono" data-tip="The scan file this page came from">
          {page.file}
        </span>
        {page.readings > 1 && (
          <span data-tip="Every reading of this page is kept. The one shown is the one in use: yours, if you corrected it.">
            {' '}
            · {plural(page.readings, 'reading')} kept
          </span>
        )}
      </div>
      {state}
      <div
        key={`${page.id}-${page.readings}-${page.state}`}
        className="tx"
        ref={textRef}
        contentEditable={!readonly}
        suppressContentEditableWarning
        role={readonly ? undefined : 'textbox'}
        aria-multiline={readonly ? undefined : true}
        spellCheck={false}
        aria-label={`Transcription of ${label}`}
      >
        {marked(page.text, unsure)}
      </div>
      {unsure.length > 0 && <p className="note">Words Lindley wasn’t sure of are highlighted and underlined with dots.</p>}
      {!readonly && actions && <div className="rv-acts">{actions}</div>}
    </>
  )
}
