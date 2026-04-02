// Mock data for testing the UI without backend connection

export interface Folder {
  id: string;
  name: string;
  icon: string;
  count?: number;
  children?: Folder[];
}

export interface Document {
  id: string;
  name: string;
  pages: number;
  currentPage: number;
  thumbnail?: string;
}

export interface ChatMessage {
  id: string;
  sender: 'user' | 'ai';
  message: string;
  timestamp: Date;
}

// Folder structure
export const mockFolders: Folder[] = [
  {
    id: 'inbox',
    name: 'Inbox',
    icon: '📁',
    count: 12,
  },
  {
    id: 'in-process',
    name: 'In Process',
    icon: '📁',
    count: 8,
  },
  {
    id: 'documents',
    name: 'Documents',
    icon: '📂',
    children: [
      {
        id: 'letters',
        name: 'Letters',
        icon: '📁',
        count: 5,
      },
      {
        id: 'invoices',
        name: 'Invoices',
        icon: '📁',
        count: 3,
      },
      {
        id: 'contracts',
        name: 'Contracts',
        icon: '📁',
        count: 2,
      },
    ],
  },
];

// Sample document
export const mockDocument: Document = {
  id: 'doc-001',
  name: 'Sample Document.pdf',
  pages: 15,
  currentPage: 3,
  thumbnail: 'https://via.placeholder.com/400x500?text=Page+3',
};

// Sample chat messages
export const mockChatMessages: ChatMessage[] = [
  {
    id: 'msg-1',
    sender: 'user',
    message: 'What is this document about?',
    timestamp: new Date(Date.now() - 5 * 60000),
  },
  {
    id: 'msg-2',
    sender: 'ai',
    message: 'This appears to be a letter dated January 15, 2026. It contains information about account management and includes several important dates and references.',
    timestamp: new Date(Date.now() - 4 * 60000),
  },
  {
    id: 'msg-3',
    sender: 'user',
    message: 'Can you extract the key dates?',
    timestamp: new Date(Date.now() - 3 * 60000),
  },
  {
    id: 'msg-4',
    sender: 'ai',
    message: 'Based on the document, the key dates are:\n- January 15, 2026: Document date\n- February 1, 2026: Payment due date\n- March 31, 2026: Final deadline',
    timestamp: new Date(Date.now() - 2 * 60000),
  },
  {
    id: 'msg-5',
    sender: 'user',
    message: 'Thanks! Can you move this to the Invoices folder?',
    timestamp: new Date(Date.now() - 60000),
  },
  {
    id: 'msg-6',
    sender: 'ai',
    message: 'I\'ve moved the document to the Invoices folder. Is there anything else you\'d like me to help with?',
    timestamp: new Date(Date.now()),
  },
];
