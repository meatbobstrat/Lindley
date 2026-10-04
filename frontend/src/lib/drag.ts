// What's being dragged inside the app: pages (onto a document, the Inbox or Set aside) or a
// document (onto a folder). A drag's data can't be read until it's dropped, so it's kept here.

export const drag: { pages: number[] | null; doc: number | null } = { pages: null, doc: null }
