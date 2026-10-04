// A folder of yours: its subfolders and documents. Each document lives in one place, so filing
// one here takes it out of In progress or Completed.

import { useState } from 'react'
import { useNavigate, useParams } from 'react-router'
import { api, imageAt } from '../api/client'
import { Head, NameInput } from '../components/bits'
import { useActions } from '../lib/actionsContext'
import { useApp, useLooking } from '../lib/appContext'
import { drag } from '../lib/drag'
import { docDate, plural } from '../lib/words'
import { DBtn, Dock } from '../ui/Dock'
import { useFeedback } from '../ui/feedbackContext'
import { Icon } from '../ui/icons'

export function FolderView() {
  const id = Number(useParams().id)
  const { folders, docs, folderPath, overview } = useApp()
  const acts = useActions()
  const { run } = useFeedback()
  const nav = useNavigate()
  const f = folders.get(id)
  const [over, setOver] = useState<number | null>(null)
  useLooking(f?.name ?? 'A folder')

  if (!f) return overview ? <div className="empty">That folder isn’t there any more.</div> : null

  const kids = [...folders.values()].filter((k) => k.parent_id === id)
  const inside = [...docs.values()].filter((d) => d.folder_id === id)
  const count = (fid: number): number =>
    [...docs.values()].filter((d) => d.folder_id === fid).length + [...folders.values()].filter((k) => k.parent_id === fid).reduce((n, k) => n + count(k.id), 0)

  return (
    <>
      <Head
        crumbs={['My folders', ...folderPath(f.parent_id)].join(' › ')}
        title={
          <>
            <h1 className="sr-only">{f.name}</h1>
            <NameInput
              key={f.name}
              value={f.name}
              label="Folder name"
              tip="Click to rename the folder. Enter saves, Escape puts it back."
              onSave={(v) => run(api.renameFolder(id, v), 'Renamed the folder.')}
            />
          </>
        }
        sub={`${plural(inside.length, 'document')}${kids.length ? ` and ${plural(kids.length, 'subfolder')}` : ''}. Each document lives in one place, so moving one here takes it out of In progress or Completed.`}
      />
      <div className="scroll">
        <div className="docs">
          {kids.map((k) => (
            <button
              key={k.id}
              className={`dcard${over === k.id ? ' is-drop' : ''}`}
              onClick={() => nav(`/folders/${k.id}`)}
              onDragOver={(e) => {
                if (drag.doc == null) return
                e.preventDefault()
                setOver(k.id)
              }}
              onDragLeave={() => setOver(null)}
              onDrop={(e) => {
                e.preventDefault()
                setOver(null)
                const d = drag.doc != null ? docs.get(drag.doc) : undefined
                drag.doc = null
                if (d) acts.fileDoc(d, k.id)
              }}
              data-tip={`Open ${k.name}. Drop a document here to file it there.`}
            >
              <span className="pg-img" style={{ display: 'grid', placeItems: 'center' }}>
                <Icon name="folder" />
              </span>
              <span>
                <b>{k.name}</b>
                <small>{plural(count(k.id), 'document')}</small>
              </span>
            </button>
          ))}
          {inside.map((d) => (
            <button
              key={d.id}
              className="dcard"
              onClick={() => nav(`/documents/${d.id}`)}
              draggable
              onDragStart={(e) => {
                drag.doc = d.id
                e.dataTransfer.effectAllowed = 'move'
                e.dataTransfer.setData('text/plain', d.name)
              }}
              onDragEnd={() => (drag.doc = null)}
              data-tip={`Open “${d.name}”. Drag it onto another folder to move it.`}
            >
              <span className="pg-img">{d.first_page != null && <img src={imageAt(`/api/pages/${d.first_page}/image`, 200)} alt="" loading="lazy" />}</span>
              <span>
                <b style={d.suggested ? { fontStyle: 'italic' } : undefined}>{d.name}</b>
                <small>
                  <Icon name={d.status === 'complete' ? 'checkc' : 'doc'} />
                  {d.status === 'complete' ? 'Completed' : d.ready ? 'Ready to export' : 'In progress'}
                </small>
                {d.to_review > 0 && (
                  <small>
                    <Icon name="flag" /> {plural(d.to_review, 'page')} to review
                  </small>
                )}
                <small>
                  {plural(d.pages, 'page')}
                  {d.doc_date ? ` · ${docDate(d.doc_date)}` : ''}
                </small>
              </span>
            </button>
          ))}
          {!inside.length && !kids.length && (
            <div className="empty">This folder is empty. Drag a document here from the list on the left, or open one and choose Move to folder.</div>
          )}
        </div>
      </div>
      <Dock label="Folder actions">
        <DBtn icon="plus" label="New subfolder" tip={`Make a folder inside ${f.name}`} onClick={() => acts.newFolder(id)} />
      </Dock>
    </>
  )
}
