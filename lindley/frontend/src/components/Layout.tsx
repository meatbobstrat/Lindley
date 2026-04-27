import { useState } from 'react';
import FolderTree from './FolderTree';
import DocumentViewer from './DocumentViewer';
import AIChat from './AIChat';
import SettingsPanel from './SettingsPanel';
import { mockFolders, mockDocument, mockChatMessages } from '../mockData';
import type { Folder, ChatMessage } from '../mockData';

export default function Layout() {
  const [selectedFolder, setSelectedFolder] = useState<Folder | null>(null);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>(mockChatMessages);
  const [activeView, setActiveView] = useState<'main' | 'settings'>('main');

  const handleSelectFolder = (folder: Folder) => {
    setSelectedFolder(folder);
  };

  const handleSendMessage = (message: string) => {
    // Add user message
    const userMessage: ChatMessage = {
      id: `msg-${Date.now()}`,
      sender: 'user',
      message,
      timestamp: new Date(),
    };

    setChatMessages([...chatMessages, userMessage]);

    // Simulate AI response after a short delay
    setTimeout(() => {
      const aiMessage: ChatMessage = {
        id: `msg-${Date.now()}-ai`,
        sender: 'ai',
        message: 'I understand. How can I help you further with this document?',
        timestamp: new Date(),
      };
      setChatMessages((prev) => [...prev, aiMessage]);
    }, 500);
  };

  return (
    <div className="h-screen w-screen bg-gray-900 flex flex-col">
      {/* Title Bar */}
      <div className="bg-gray-950 border-b border-gray-700 px-6 py-4 flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white">📚 Lindley Archives</h1>
        <div className="flex gap-2">
          <button
            onClick={() => setActiveView(activeView === 'main' ? 'settings' : 'main')}
            className="px-4 py-2 rounded hover:bg-gray-700 text-gray-400 hover:text-white transition-colors"
            title="Settings"
          >
            {activeView === 'settings' ? '📚 Back' : '⚙️ Settings'}
          </button>
          <button className="w-8 h-8 rounded hover:bg-gray-700 text-gray-400 hover:text-white transition-colors">
            −
          </button>
          <button className="w-8 h-8 rounded hover:bg-gray-700 text-gray-400 hover:text-white transition-colors">
            □
          </button>
          <button className="w-8 h-8 rounded hover:bg-red-600 text-gray-400 hover:text-white transition-colors">
            ×
          </button>
        </div>
      </div>

      {/* Main or Settings View */}
      {activeView === 'settings' ? (
        <SettingsPanel />
      ) : (
        <>
          {/* Original main layout */}
          <div className="flex-1 flex overflow-hidden">
            {/* Sidebar - Folder Tree */}
            <div className="w-64 flex-shrink-0 overflow-hidden">
              <FolderTree
                folders={mockFolders}
                onSelectFolder={handleSelectFolder}
                selectedFolderId={selectedFolder?.id}
              />
            </div>

            {/* Main Content - Document Viewer and Chat */}
            <div className="flex-1 flex flex-col overflow-hidden">
              {/* Document Viewer */}
              <div className="flex-1 overflow-hidden">
                <DocumentViewer
                  document={mockDocument}
                  onPreviousPage={() => console.log('Previous page')}
                  onNextPage={() => console.log('Next page')}
                  onRotate={() => console.log('Rotate')}
                  onFlipHorizontal={() => console.log('Flip horizontal')}
                  onMove={() => console.log('Move')}
                  onDelete={() => console.log('Delete')}
                />
              </div>

              {/* AI Chat */}
              <div className="h-56 flex-shrink-0 overflow-hidden">
                <AIChat messages={chatMessages} onSendMessage={handleSendMessage} />
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
