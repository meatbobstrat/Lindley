import { useState } from 'react';
import type { Folder } from '../mockData';

interface FolderTreeProps {
  folders: Folder[];
  onSelectFolder: (folder: Folder) => void;
  selectedFolderId?: string;
}

export default function FolderTree({ folders, onSelectFolder, selectedFolderId }: FolderTreeProps) {
  const [expandedFolders, setExpandedFolders] = useState<Set<string>>(new Set(['documents']));

  const toggleExpand = (folderId: string) => {
    const newExpanded = new Set(expandedFolders);
    if (newExpanded.has(folderId)) {
      newExpanded.delete(folderId);
    } else {
      newExpanded.add(folderId);
    }
    setExpandedFolders(newExpanded);
  };

  const renderFolder = (folder: Folder, level: number = 0) => {
    const isExpanded = expandedFolders.has(folder.id);
    const hasChildren = folder.children && folder.children.length > 0;
    const isSelected = selectedFolderId === folder.id;

    return (
      <div key={folder.id}>
        <div
          onClick={() => {
            onSelectFolder(folder);
            if (hasChildren) {
              toggleExpand(folder.id);
            }
          }}
          className={`
            flex items-center gap-2 px-3 py-2 cursor-pointer rounded transition-colors
            ${isSelected 
              ? 'bg-blue-600 text-white' 
              : 'text-gray-200 hover:bg-gray-700'
            }
          `}
          style={{ paddingLeft: `${level * 16 + 12}px` }}
        >
          {hasChildren && (
            <span className="text-xs">
              {isExpanded ? '▼' : '▶'}
            </span>
          )}
          {!hasChildren && <span className="text-xs w-4"></span>}
          <span className="text-lg">{folder.icon}</span>
          <span className="flex-1 text-sm font-medium">{folder.name}</span>
          {folder.count !== undefined && (
            <span className="text-xs bg-gray-600 px-2 py-1 rounded-full">
              {folder.count}
            </span>
          )}
        </div>

        {hasChildren && isExpanded && (
          <div>
            {folder.children!.map((child) => renderFolder(child, level + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="h-full bg-gray-900 border-r border-gray-700 overflow-y-auto">
      <div className="p-4">
        <h2 className="text-lg font-bold text-white mb-4">📂 Folders</h2>
        <div className="space-y-1">
          {folders.map((folder) => renderFolder(folder))}
        </div>
      </div>
    </div>
  );
}
