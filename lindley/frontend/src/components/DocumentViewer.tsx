import type { Document } from '../mockData';

interface DocumentViewerProps {
  document?: Document;
  onPreviousPage?: () => void;
  onNextPage?: () => void;
  onRotate?: () => void;
  onFlipHorizontal?: () => void;
  onMove?: () => void;
  onDelete?: () => void;
}

export default function DocumentViewer({
  document,
  onPreviousPage,
  onNextPage,
  onRotate,
  onFlipHorizontal,
  onMove,
  onDelete,
}: DocumentViewerProps) {
  return (
    <div className="h-full bg-gray-800 flex flex-col">
      {/* Toolbar */}
      <div className="bg-gray-900 border-b border-gray-700 px-4 py-3 flex items-center gap-2">
        <button
          onClick={onPreviousPage}
          disabled={!document}
          className="p-2 rounded hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed text-gray-200 hover:text-white transition-colors"
          title="Previous Page"
        >
          ◀
        </button>
        <button
          onClick={onNextPage}
          disabled={!document}
          className="p-2 rounded hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed text-gray-200 hover:text-white transition-colors"
          title="Next Page"
        >
          ▶
        </button>
        <button
          onClick={onRotate}
          disabled={!document}
          className="p-2 rounded hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed text-gray-200 hover:text-white transition-colors"
          title="Rotate"
        >
          ↻
        </button>
        <button
          onClick={onFlipHorizontal}
          disabled={!document}
          className="p-2 rounded hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed text-gray-200 hover:text-white transition-colors"
          title="Flip Horizontal"
        >
          ↔
        </button>

        <div className="flex-1"></div>

        {document && (
          <span className="text-sm text-gray-400">
            Page {document.currentPage} of {document.pages}
          </span>
        )}

        <div className="flex-1"></div>

        <button
          onClick={onMove}
          disabled={!document}
          className="px-3 py-1 rounded bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm transition-colors"
          title="Move to Folder"
        >
          Move
        </button>
        <button
          onClick={onDelete}
          disabled={!document}
          className="px-3 py-1 rounded bg-red-600 hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm transition-colors"
          title="Delete"
        >
          Delete
        </button>
      </div>

      {/* Document Display Area */}
      <div className="flex-1 flex items-center justify-center overflow-auto">
        {document ? (
          <div className="flex flex-col items-center gap-4">
            <img
              src={document.thumbnail}
              alt={document.name}
              className="max-w-full max-h-full object-contain rounded shadow-lg"
            />
            <p className="text-gray-400 text-sm">{document.name}</p>
          </div>
        ) : (
          <div className="text-center">
            <p className="text-gray-400 text-lg">📄 Select a document to view</p>
            <p className="text-gray-500 text-sm mt-2">Choose a document from the folder tree</p>
          </div>
        )}
      </div>
    </div>
  );
}
