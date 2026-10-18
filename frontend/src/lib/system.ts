// Folder paths in the words of the system Lindley runs on: Windows, the Mac or Ubuntu.

import { api, type System } from '../api/client'
import { useApi } from '../api/store'

const COPY: Record<System, string> = {
  windows: 'Copy it from File Explorer’s address bar.',
  mac: 'In Finder, hold Option and right-click the folder, then choose Copy as Pathname.',
  linux: 'In Files, open the folder and press Ctrl+L to show its path, then copy it.',
}

const TESSERACT: Record<System, string> = {
  windows: 'C:\\Program Files\\Tesseract-OCR\\tesseract.exe',
  mac: '/opt/homebrew/bin/tesseract',
  linux: '/usr/bin/tesseract',
}

/** An example folder, where to copy a path from, and where Tesseract usually is. Until Lindley
 * has said which system it's on, nothing that belongs to one. */
export function useSystem() {
  const h = useApi('health', api.health).data
  return {
    folder: h?.example_folder,
    copyPath: h ? COPY[h.system] : '',
    tesseract: h ? TESSERACT[h.system] : undefined,
  }
}
