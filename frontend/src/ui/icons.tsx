// Line icons, and the Lindley mark: one shape that always means "this gets you Lindley, the AI".

const PATHS = {
  inbox: 'M3 13h5l2 3h4l2-3h5M5.5 5h13L21 13v6H3v-6z',
  folder: 'M3 6.5h6l2 2h10V19H3z',
  doc: 'M6 3h9l4 4v14H6zM15 3v4h4',
  dup: 'M9 3h8l3 3v12H9zM17 3v3h3M5 7v14h11',
  check: 'M5 12.5l4.5 4.5L19 7',
  aside: 'M3 5h18v4H3zM5 9v10h14V9M10 13h4',
  rotL: 'M4 5v5h5M4.6 10a8 8 0 1 1 1.6 6.5',
  rotR: 'M20 5v5h-5M19.4 10a8 8 0 1 0-1.6 6.5',
  flip: 'M12 3v18M9 7L3 17h6zM15 7l6 10h-6z',
  up: 'M12 19V5M6 11l6-6 6 6',
  down: 'M12 5v14M6 13l6 6 6-6',
  move: 'M3 7h7l2 2h9v10H3zM11 14h6M14 11l3 3-3 3',
  flag: 'M5 21V4M5 4h12l-2.5 4L17 12H5',
  warn: 'M12 3.5l9.5 17h-19zM12 10v4.5M12 17.5v.01',
  checkc: 'M12 3a9 9 0 1 1 0 18 9 9 0 0 1 0-18zM8 12.3l2.8 2.8L16.2 9.5',
  popout: 'M14 4h6v6M20 4l-8 8M18 14v6H4V6h6',
  dock: 'M4 4h16v16H4zM14 4v16',
  close: 'M6 6l12 12M18 6L6 18',
  chev: 'M9 6l6 6-6 6',
  prev: 'M15 6l-6 6 6 6',
  menu: 'M4 7h16M4 12h16M4 17h16',
  sun: 'M12 8a4 4 0 1 1 0 8 4 4 0 0 1 0-8zM12 2v2M12 20v2M2 12h2M20 12h2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4',
  moon: 'M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z',
  zin: 'M11 4a7 7 0 1 1 0 14 7 7 0 0 1 0-14zM16 16l5 5M8 11h6M11 8v6',
  zout: 'M11 4a7 7 0 1 1 0 14 7 7 0 0 1 0-14zM16 16l5 5M8 11h6',
  send: 'M4 12l16-8-6 16-2.5-6.5z',
  plus: 'M12 5v14M5 12h14',
  pdf: 'M12 4v11M7 10l5 5 5-5M5 20h14',
  eye: 'M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12zM12 9a3 3 0 1 1 0 6 3 3 0 0 1 0-6z',
  newdoc: 'M6 3h9l4 4v14H6zM12 10v7M8.5 13.5h7',
  back: 'M9 7L4 12l5 5M4 12h16',
  more: 'M6 9l6 6 6-6',
  pen: 'M4 20h4L19 9l-4-4L4 16zM13.5 6.5l4 4',
  grip: 'M9 6h.01M15 6h.01M9 12h.01M15 12h.01M9 18h.01M15 18h.01',
  upload: 'M12 16V4M7 9l5-5 5 5M5 20h14',
  lock: 'M6 11h12v9H6zM8.5 11V8a3.5 3.5 0 0 1 7 0v3M12 14.5v2',
  cloud: 'M7 18.5h10.5a4 4 0 0 0 .4-8 6 6 0 0 0-11.6-1A4.8 4.8 0 0 0 7 18.5z',
  gear: 'M10.3 3h3.4l.5 2.4 1.7 1 2.3-.8 1.7 2.9-1.8 1.6v2l1.8 1.6-1.7 2.9-2.3-.8-1.7 1-.5 2.4h-3.4l-.5-2.4-1.7-1-2.3.8-1.7-2.9L5.6 13v-2L3.8 9.4l1.7-2.9 2.3.8 1.7-1zM12 9.5a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5z',
  search: 'M11 4a7 7 0 1 1 0 14 7 7 0 0 1 0-14zM16 16l5 5',
} as const

export type IconName = keyof typeof PATHS | 'lindley'

export function Icon({ name, className = '' }: { name: IconName; className?: string }) {
  if (name === 'lindley') return <Mark className={className} />
  return (
    <svg className={`ic ${className}`} viewBox="0 0 24 24" aria-hidden="true">
      <path d={PATHS[name]} />
    </svg>
  )
}

export function Mark({ className = '' }: { className?: string }) {
  return (
    <svg className={`lic ${className}`} aria-hidden="true">
      <use href="#lindley" />
    </svg>
  )
}

/** The mark's shape, defined once for the page: a speech bubble with an "L" cut out of it. */
export function MarkDefs() {
  return (
    <svg width="0" height="0" style={{ position: 'absolute' }} aria-hidden="true" focusable="false">
      <defs>
        <mask id="lindley-cut" maskUnits="userSpaceOnUse" x="0" y="0" width="24" height="24">
          <rect width="24" height="24" fill="#fff" />
          <path d="M9.4 6.6v9.6h6.2" fill="none" stroke="#000" strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round" />
        </mask>
        <symbol id="lindley" viewBox="0 0 24 24">
          <path
            d="M12 1.8a10.2 10.2 0 1 1-4.6 19.3L2.6 22.4a.6.6 0 0 1-.7-.8l1.5-4.3A10.2 10.2 0 0 1 12 1.8z"
            fill="currentColor"
            mask="url(#lindley-cut)"
          />
        </symbol>
      </defs>
    </svg>
  )
}
