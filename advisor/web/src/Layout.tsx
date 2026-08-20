import { Activity, Cpu, Database, FileUp, Gauge, KeyRound, LogOut, Network, Route, Server, Shield, SlidersHorizontal } from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { logout } from './api/client'
import styles from './Layout.module.css'

/**
 * Shell for every authenticated route -- icon-only dark nav rail + a breadcrumb top bar, same
 * shape as Omnigate's AdminLayout (~/Projects/Omnigate/web/src/pages/admin/AdminLayout.tsx).
 * Four nav destinations: Connections (per-database work, live connect strings), Reports
 * (upload-a-report on-ramp for customers who won't share a live connect string), Sizing
 * (Postgres instance sizing built from either), and LLM configuration (app-wide, not scoped to
 * any of the above).
 */
export default function Layout({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate()

  async function handleLogout() {
    await logout()
    navigate('/login')
  }

  return (
    <div className={styles.shell}>
      <nav className={styles.rail}>
        <div className={styles.railMark}>PG</div>
        <div className={styles.railGroup}>
          <NavLink
            to="/connections"
            title="Connections"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Database size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Connections</span>
          </NavLink>
          <NavLink
            to="/reports"
            title="Reports"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <FileUp size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Reports</span>
          </NavLink>
          <NavLink
            to="/sizing"
            title="Sizing"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Gauge size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Sizing</span>
          </NavLink>
          <NavLink
            to="/llm-settings"
            title="LLM configuration"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Cpu size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>LLM configuration</span>
          </NavLink>
          <NavLink
            to="/wire-metrics"
            title="Wire: Metrics"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Activity size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Wire: Metrics</span>
          </NavLink>
          <NavLink
            to="/wire-firewall"
            title="Wire: SQL Firewall"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Shield size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Wire: SQL Firewall</span>
          </NavLink>
          <NavLink
            to="/wire-acl"
            title="Wire: ACL"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Network size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Wire: ACL</span>
          </NavLink>
          <NavLink
            to="/wire-backends"
            title="Wire: Backends"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Server size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Wire: Backends</span>
          </NavLink>
          <NavLink
            to="/wire-router"
            title="Wire: Router rules"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <Route size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Wire: Router rules</span>
          </NavLink>
          <NavLink
            to="/wire-qos"
            title="Wire: QoS"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <SlidersHorizontal size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Wire: QoS</span>
          </NavLink>
          <NavLink
            to="/wire-oauth"
            title="Wire: OAuth"
            className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}
          >
            <KeyRound size={19} strokeWidth={1.8} />
            <span className={styles.railTip}>Wire: OAuth</span>
          </NavLink>
        </div>
        <div className={styles.railSpacer} />
        <button
          onClick={handleLogout}
          title="Sign out"
          className={styles.railItem}
          style={{ background: 'none', border: 'none' }}
        >
          <LogOut size={19} strokeWidth={1.8} />
          <span className={styles.railTip}>Sign out</span>
        </button>
      </nav>
      <div className={styles.main}>
        <div className={styles.topbar}>
          <div className={styles.crumb}>polygres advisor</div>
        </div>
        <div className={styles.content}>{children}</div>
      </div>
    </div>
  )
}
