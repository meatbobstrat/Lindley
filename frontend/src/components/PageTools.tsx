// The toolbar buttons every view that shows pages offers for them: turn, flip, and More…, the
// page menu (so it's there without right-clicking). One set, so no view is missing one.

import type { ReactNode } from 'react'
import { useActions } from '../lib/actionsContext'
import { DBtn } from '../ui/Dock'

const AGAIN = 'The scan itself isn’t changed; its text is read again the new way round.'

function moreTip(where: 'inbox' | 'aside' | 'document', n: number) {
  if (where === 'document') return 'Move to another document, a new one, the Inbox or Set aside'
  const it = n === 1 ? 'it' : 'them'
  return `${n === 1 ? 'Open it, add' : 'Add'} ${it} to a document or a new one, or ${where === 'inbox' ? `set ${it} aside` : `return ${it} to the Inbox`}`
}

export function PageTools({
  ids,
  where,
  noun = 'page',
  docId,
  more = true,
  children,
}: {
  ids: number[]
  where: 'inbox' | 'aside' | 'document'
  noun?: 'page' | 'scan'
  docId?: number
  /** Whether to offer More… (not where every item in it is on the toolbar already). */
  more?: boolean
  /** Buttons that go after Flip, before More… */
  children?: ReactNode
}) {
  const acts = useActions()
  const n = ids.length
  const select = (what: string) => `Select ${noun}s to ${what}`
  return (
    <>
      <DBtn icon="rotL" label="Turn left" iconOnly disabled={!n} tip={n ? `A quarter turn left. ${AGAIN}` : select('turn them')} onClick={() => acts.rotate(ids, -90)} />
      <DBtn icon="rotR" label="Turn right" iconOnly disabled={!n} tip={n ? `A quarter turn right. ${AGAIN}` : select('turn them')} onClick={() => acts.rotate(ids, 90)} />
      <DBtn
        icon="flip"
        label="Flip left to right"
        iconOnly
        disabled={!n}
        tip={n ? `For a mirror image, such as the back of a carbon copy. ${AGAIN}` : select('flip them')}
        onClick={() => acts.flip(ids)}
      />
      {children}
      {more && (
        <DBtn
          icon="more"
          label="More…"
          disabled={!n}
          tip={n ? moreTip(where, n) : select('see more')}
          onClick={(e) => acts.pageMenu(ids, where, e.currentTarget, { docId, noBasics: true })}
        />
      )}
    </>
  )
}
