import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { ConceptExclusionProvider } from './share/conceptExclusions'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ConceptExclusionProvider>
      <App />
    </ConceptExclusionProvider>
  </StrictMode>,
)
