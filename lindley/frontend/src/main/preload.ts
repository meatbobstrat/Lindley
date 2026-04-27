import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('electronAPI', {
  selectDirectory: () => ipcRenderer.invoke('select-directory'),
});

declare global {
  interface Window {
    electronAPI: {
      selectDirectory: () => Promise<string | null>;
    };
  }
}
