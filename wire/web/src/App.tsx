import { Navigate, Route, Routes } from 'react-router-dom'
import { getStoredConnection } from './api/client'
import Layout from './Layout'
import Connect from './pages/Connect'
import Dashboard from './pages/Dashboard'
import Metrics from './pages/Metrics'
import Topology from './pages/Topology'
import FirewallRules from './pages/FirewallRules'
import AclRules from './pages/AclRules'
import OAuth from './pages/OAuth'
import Backends from './pages/Backends'
import Queues from './pages/Queues'
import DataExplorer from './pages/DataExplorer'
import RouterRules from './pages/RouterRules'
import Qos from './pages/Qos'
import LlmConfig from './pages/LlmConfig'
import FederationPlans from './pages/FederationPlans'

/** Gate: redirects to /connect unless a base URL + token are already sitting in sessionStorage.
 * Unlike advisor's RequireAuth, this never calls the server to check -- Warp's admin API has
 * no session-check endpoint (no session machinery at all, by design), so "connected" just means
 * "we have a token stored"; an actually-invalid token is caught by the first 401 any page hits,
 * which clears storage and bounces back here (see src/api/client.ts). */
function RequireAuth({ children }: { children: React.ReactNode }) {
  if (!getStoredConnection()) return <Navigate to="/connect" replace />
  return <Layout>{children}</Layout>
}

export default function App() {
  return (
    <Routes>
      <Route path="/connect" element={<Connect />} />
      <Route path="/dashboard" element={<RequireAuth><Dashboard /></RequireAuth>} />
      <Route path="/metrics" element={<RequireAuth><Metrics /></RequireAuth>} />
      <Route path="/topology" element={<RequireAuth><Topology /></RequireAuth>} />
      <Route path="/firewall" element={<RequireAuth><FirewallRules /></RequireAuth>} />
      <Route path="/acl" element={<RequireAuth><AclRules /></RequireAuth>} />
      <Route path="/oauth" element={<RequireAuth><OAuth /></RequireAuth>} />
      <Route path="/backends" element={<RequireAuth><Backends /></RequireAuth>} />
      <Route path="/queues" element={<RequireAuth><Queues /></RequireAuth>} />
      <Route path="/data" element={<RequireAuth><DataExplorer /></RequireAuth>} />
      <Route path="/router" element={<RequireAuth><RouterRules /></RequireAuth>} />
      <Route path="/qos" element={<RequireAuth><Qos /></RequireAuth>} />
      <Route path="/federation-plans" element={<RequireAuth><FederationPlans /></RequireAuth>} />
      <Route path="/llm-config" element={<RequireAuth><LlmConfig /></RequireAuth>} />
      <Route path="/" element={<Navigate to="/dashboard" replace />} />
    </Routes>
  )
}
