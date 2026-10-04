// Search results: every page whose text has the words, wherever it is.

import { useNavigate, useSearchParams } from 'react-router'
import { api, imageAt } from '../api/client'
import { useApi } from '../api/store'
import { ErrorBox, Head, Loading } from '../components/bits'
import { useLooking } from '../lib/appContext'
import { plural } from '../lib/words'

export function SearchView() {
  const [params] = useSearchParams()
  const q = params.get('q') ?? ''
  const res = useApi(q ? `search:${q}` : null, () => api.search(q))
  const nav = useNavigate()
  useLooking(`Search for “${q}”`)

  if (!q) return <Head crumbs="Search" title="Search" sub="Type words in the search box at the top, then press Enter." />
  if (res.error && !res.data) return <ErrorBox error={res.error} />
  if (!res.data) return <Loading what="Searching…" />
  const r = res.data.results
  return (
    <>
      <Head
        crumbs="Search"
        title={`${plural(r.length, 'page')}${r.length === 50 ? ' or more' : ''} mention “${q}”`}
        sub="Searches the text of every page as it reads now: your corrections, handwriting read by the AI vision model, and pages still in the Inbox or set aside. Best matches first."
      />
      <div className="scroll">
        <div className="results">
          {r.length ? (
            r.map((x) => (
              <button
                key={x.page_id}
                className="res"
                data-tip="Open this page beside its text"
                onClick={() =>
                  nav(x.document_id ? `/documents/${x.document_id}?view=side&page=${(x.page_number ?? 1) - 1}` : `/scans/${x.page_id}`)
                }
              >
                <div className="pg-img">
                  <img src={imageAt(x.image, 160)} alt="" loading="lazy" />
                </div>
                <div>
                  <b className="res-where">
                    {x.document_name ? (
                      `${x.document_name} · page ${x.page_number}`
                    ) : (
                      <>
                        {x.where === 'aside' ? 'Set aside' : 'Inbox'} · <span className="mono">{x.file}</span>
                      </>
                    )}
                  </b>
                  <p>
                    {x.snippet.map(([t, hit], i) =>
                      hit ? (
                        <mark key={i}>{t}</mark>
                      ) : (
                        <span key={i}>
                          {i > 0 ? ' ' : ''}
                          {t}{' '}
                        </span>
                      ),
                    )}
                  </p>
                </div>
              </button>
            ))
          ) : (
            <div className="empty">No pages match. Try a shorter word, or a different spelling. Old handwriting is often spelled loosely.</div>
          )}
        </div>
      </div>
    </>
  )
}
