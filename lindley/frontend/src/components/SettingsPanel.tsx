import { useState, useEffect } from 'react';

interface WatchFolder {
  id: string;
  path: string;
  move_files: boolean;
  enabled: boolean;
  name: string;
}

interface Settings {
  watch_folders: WatchFolder[];
  processing_dir: string;
  quarantine_dir: string;
  db_path: string;
}

declare global {
  interface Window {
    electronAPI?: {
      selectDirectory: () => Promise<string | null>;
    };
  }
}

export default function SettingsPanel() {
  const [settings, setSettings] = useState<Settings | null>(null);
  const [loading, setLoading] = useState(false);
  const [newFolderPath, setNewFolderPath] = useState('');
  const [newFolderMove, setNewFolderMove] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  useEffect(() => {
    fetchSettings();
  }, []);

  const fetchSettings = async () => {
    try {
      const response = await fetch('http://localhost:5000/api/settings');
      if (!response.ok) throw new Error('Failed to fetch settings');
      const data = await response.json();
      setSettings(data);
    } catch (err) {
      setError(`Failed to load settings: ${err}`);
    }
  };

  const handleSelectFolder = async () => {
    if (window.electronAPI?.selectDirectory) {
      try {
        const path = await window.electronAPI.selectDirectory();
        if (path) {
          setNewFolderPath(path);
        }
      } catch (err) {
        setError(`Failed to select folder: ${err}`);
      }
    } else {
      // Fallback: manual entry
      alert('Folder picker not available. Please type the path manually.');
    }
  };

  const handleAddFolder = async () => {
    if (!newFolderPath.trim()) {
      setError('Please enter a folder path');
      return;
    }

    setLoading(true);
    try {
      const response = await fetch('http://localhost:5000/api/settings/watch-folders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path: newFolderPath,
          move_files: newFolderMove,
          name: newFolderPath.split('/').pop() || 'New Folder',
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to add folder');
      }

      setSuccess('Folder added successfully!');
      setNewFolderPath('');
      fetchSettings();
    } catch (err) {
      setError(`Failed to add folder: ${err}`);
    } finally {
      setLoading(false);
    }
  };

  const handleRemoveFolder = async (folderId: string) => {
    if (!window.confirm('Remove this watch folder?')) return;

    try {
      const response = await fetch(
        `http://localhost:5000/api/settings/watch-folders/${folderId}`,
        { method: 'DELETE' }
      );
      if (!response.ok) throw new Error('Failed to remove folder');
      setSuccess('Folder removed');
      fetchSettings();
    } catch (err) {
      setError(`Failed to remove folder: ${err}`);
    }
  };

  const handleToggleMoveFiles = async (folder: WatchFolder) => {
    try {
      const response = await fetch(
        `http://localhost:5000/api/settings/watch-folders/${folder.id}`,
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ move_files: !folder.move_files }),
        }
      );
      if (!response.ok) throw new Error('Failed to update folder');
      fetchSettings();
    } catch (err) {
      setError(`Failed to update folder: ${err}`);
    }
  };

  return (
    <div className="h-full bg-gray-900 flex flex-col overflow-auto">
      <div className="bg-gray-950 border-b border-gray-700 px-6 py-4">
        <h2 className="text-2xl font-bold text-white">⚙️ Settings</h2>
      </div>

      <div className="flex-1 p-6 overflow-y-auto">
        {error && (
          <div className="mb-4 p-4 bg-red-900 text-red-100 rounded">
            {error}
            <button
              onClick={() => setError('')}
              className="ml-4 underline hover:no-underline"
            >
              Dismiss
            </button>
          </div>
        )}

        {success && (
          <div className="mb-4 p-4 bg-green-900 text-green-100 rounded">
            {success}
            <button
              onClick={() => setSuccess('')}
              className="ml-4 underline hover:no-underline"
            >
              Dismiss
            </button>
          </div>
        )}

        <section className="mb-8">
          <h3 className="text-lg font-semibold text-white mb-4">Watched Folders</h3>

          {settings?.watch_folders && settings.watch_folders.length > 0 ? (
            <div className="space-y-3">
              {settings.watch_folders.map((folder) => (
                <div
                  key={folder.id}
                  className="bg-gray-800 border border-gray-700 p-4 rounded"
                >
                  <div className="flex items-center justify-between mb-2">
                    <div>
                      <p className="text-white font-medium">{folder.name}</p>
                      <p className="text-gray-400 text-sm">{folder.path}</p>
                    </div>
                    <button
                      onClick={() => handleRemoveFolder(folder.id)}
                      className="px-3 py-1 bg-red-600 hover:bg-red-700 text-white rounded text-sm"
                    >
                      Remove
                    </button>
                  </div>

                  <div className="flex items-center gap-4">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={folder.move_files}
                        onChange={() => handleToggleMoveFiles(folder)}
                        className="w-4 h-4"
                      />
                      <span className="text-sm text-gray-300">
                        {folder.move_files
                          ? 'Move files to inbox'
                          : 'Copy files to inbox'}
                      </span>
                    </label>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-gray-400">No watched folders configured</p>
          )}
        </section>

        <section>
          <h3 className="text-lg font-semibold text-white mb-4">Add New Folder</h3>

          <div className="bg-gray-800 border border-gray-700 p-4 rounded space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Folder Path
              </label>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={newFolderPath}
                  onChange={(e) => setNewFolderPath(e.target.value)}
                  placeholder="/path/to/scans or \\\\network\share"
                  className="flex-1 bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                />
                <button
                  onClick={handleSelectFolder}
                  className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded"
                >
                  Browse
                </button>
              </div>
            </div>

            <div>
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={newFolderMove}
                  onChange={(e) => setNewFolderMove(e.target.checked)}
                  className="w-4 h-4"
                />
                <span className="text-sm text-gray-300">
                  Move files to inbox (uncheck to copy only)
                </span>
              </label>
            </div>

            <button
              onClick={handleAddFolder}
              disabled={loading}
              className="w-full px-4 py-2 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white rounded font-medium"
            >
              {loading ? 'Adding...' : 'Add Folder'}
            </button>
          </div>
        </section>
      </div>
    </div>
  );
}
