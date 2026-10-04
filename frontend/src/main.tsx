import '@fontsource/ibm-plex-sans/400.css'
import '@fontsource/ibm-plex-sans/500.css'
import '@fontsource/ibm-plex-sans/600.css'
import '@fontsource/ibm-plex-mono/400.css'
import '@fontsource/ibm-plex-mono/500.css'
import '@fontsource/source-serif-4/400.css'
import '@fontsource/source-serif-4/600.css'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router'
import './index.css'
import './app.css'
import App from './App.tsx'
import { ActionsProvider } from './lib/actions'
import { AppProvider } from './lib/app'
import { FeedbackProvider } from './ui/feedback'

// A data router, so Settings can ask before you leave it with changes unsaved (useBlocker).
// The fonts are bundled, so Lindley never asks another site for anything.
const router = createBrowserRouter([
  {
    path: '*',
    element: (
      <AppProvider>
        <FeedbackProvider>
          <ActionsProvider>
            <App />
          </ActionsProvider>
        </FeedbackProvider>
      </AppProvider>
    ),
  },
])

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
)
