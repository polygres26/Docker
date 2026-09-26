import { Navigate, Route, Routes } from 'react-router-dom'
import { getStoredConnection } from './api/client'
import { AppShell } from './components/ui'
import Connect from './pages/Connect'
import Overview from './pages/Overview'
import Workloads from './pages/Workloads'
import Infrastructure from './pages/Infrastructure'
import SqlDrivers from './pages/interfaces/SqlDrivers'
import ApiEndpoints from './pages/interfaces/ApiEndpoints'
import McpServers from './pages/interfaces/McpServers'
import Metrics from './pages/Metrics'
import Topology from './pages/Topology'
import FirewallRules from './pages/FirewallRules'
import AclRules from './pages/AclRules'
import OAuth from './pages/OAuth'
import Queues from './pages/Queues'
import DataExplorer from './pages/DataExplorer'
import RouterRules from './pages/RouterRules'
import Qos from './pages/Qos'
import Rollups from './pages/Rollups'
import LlmConfig from './pages/LlmConfig'
import FederationPlans from './pages/FederationPlans'
import AbRouting from './pages/AbRouting'

/** Gate: redirects to /connect unless a base URL + token are already sitting in sessionStorage.
 * Unlike advisor's RequireAuth, this never calls the server to check -- Warp's admin API has
 * no session-check endpoint (no session machinery at all, by design), so "connected" just means
 * "we have a token stored"; an actually-invalid token is caught by the first 401 any page hits,
 * which clears storage and bounces back here (see src/api/client.ts). */
function RequireAuth({ children }: { children: React.ReactNode }) {
  if (!getStoredConnection()) return <Navigate to="/connect" replace />
  return <AppShell>{children}</AppShell>
}

export default function App() {
  return (
    <Routes>
      <Route path="/connect" element={<Connect />} />
      <Route path="/overview" element={<RequireAuth><Overview /></RequireAuth>} />
      <Route path="/dashboard" element={<Navigate to="/overview" replace />} />
      <Route path="/workloads" element={<RequireAuth><Workloads /></RequireAuth>} />
      <Route path="/infrastructure" element={<RequireAuth><Infrastructure /></RequireAuth>} />
      <Route path="/interfaces/sql" element={<RequireAuth><SqlDrivers /></RequireAuth>} />
      <Route path="/interfaces/api" element={<RequireAuth><ApiEndpoints /></RequireAuth>} />
      <Route path="/interfaces/mcp" element={<RequireAuth><McpServers /></RequireAuth>} />
      <Route path="/metrics" element={<RequireAuth><Metrics /></RequireAuth>} />
      <Route path="/topology" element={<RequireAuth><Topology /></RequireAuth>} />
      <Route path="/firewall" element={<RequireAuth><FirewallRules /></RequireAuth>} />
      <Route path="/acl" element={<RequireAuth><AclRules /></RequireAuth>} />
      <Route path="/oauth" element={<RequireAuth><OAuth /></RequireAuth>} />
      {/* Backends now live inside backend sets; the old page is gone, old links keep working */}
      <Route path="/backends" element={<Navigate to="/infrastructure" replace />} />
      <Route path="/backend-sets" element={<Navigate to="/infrastructure" replace />} />
      <Route path="/ab-routing" element={<RequireAuth><AbRouting /></RequireAuth>} />
      <Route path="/queues" element={<RequireAuth><Queues /></RequireAuth>} />
      <Route path="/data" element={<RequireAuth><DataExplorer /></RequireAuth>} />
      <Route path="/router" element={<RequireAuth><RouterRules /></RequireAuth>} />
      <Route path="/qos" element={<RequireAuth><Qos /></RequireAuth>} />
      <Route path="/rollups" element={<RequireAuth><Rollups /></RequireAuth>} />
      <Route path="/federation-plans" element={<RequireAuth><FederationPlans /></RequireAuth>} />
      <Route path="/llm-config" element={<RequireAuth><LlmConfig /></RequireAuth>} />
      <Route path="/" element={<Navigate to="/overview" replace />} />
      <Route path="*" element={<Navigate to="/overview" replace />} />
    </Routes>
  )
}
