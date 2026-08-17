import { useEffect, useState } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import { checkSession } from './api/client'
import Layout from './Layout'
import Login from './pages/Login'
import Connections from './pages/Connections'
import ConnectionDetail from './pages/ConnectionDetail'
import Connect from './pages/Connect'
import Report from './pages/Report'
import LlmSettings from './pages/LlmSettings'

/** Gate: redirects to /login unless a valid admin session cookie is present, then wraps the page in the rail/topbar shell. Checked once per mount via GET /api/session (never triggers a 401 itself). */
function RequireAuth({ children }: { children: React.ReactNode }) {
  const [status, setStatus] = useState<'checking' | 'authed' | 'anon'>('checking')

  useEffect(() => {
    checkSession().then((ok) => setStatus(ok ? 'authed' : 'anon')).catch(() => setStatus('anon'))
  }, [])

  if (status === 'checking') return null
  if (status === 'anon') return <Navigate to="/login" replace />
  return <Layout>{children}</Layout>
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/connections" element={<RequireAuth><Connections /></RequireAuth>} />
      <Route path="/connections/:id" element={<RequireAuth><ConnectionDetail /></RequireAuth>} />
      <Route path="/llm-settings" element={<RequireAuth><LlmSettings /></RequireAuth>} />
      <Route path="/quick-scan" element={<RequireAuth><Connect /></RequireAuth>} />
      <Route path="/report" element={<RequireAuth><Report /></RequireAuth>} />
      <Route path="/" element={<Navigate to="/connections" replace />} />
    </Routes>
  )
}
