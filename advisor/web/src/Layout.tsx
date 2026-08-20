import {
  Activity, KeyRound, LayoutGrid, LogOut, Network, Route, Server, Shield, SlidersHorizontal,
} from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { logout } from './api/client'
import styles from './Layout.module.css'

const WIRE_ITEMS = [
  { to: '/wire-metrics', label: 'Metrics', icon: Activity },
  { to: '/wire-firewall', label: 'SQL Firewall', icon: Shield },
  { to: '/wire-acl', label: 'ACL', icon: Network },
  { to: '/wire-backends', label: 'Backends', icon: Server },
  { to: '/wire-router', label: 'Router rules', icon: Route },
  { to: '/wire-qos', label: 'QoS', icon: SlidersHorizontal },
  { to: '/wire-oauth', label: 'OAuth', icon: KeyRound },
]

// Advisor's own four pages (Connections, Reports, Sizing, LLM configuration) live behind one
// sidebar entry now -- they're tabs on each other (see AdvisorTabs), not separate destinations,
// so the sidebar doesn't need to enumerate them. isActive covers every Advisor route so this
// entry stays highlighted no matter which Advisor tab is open.
const ADVISOR_ROUTES = ['/connections', '/reports', '/sizing', '/llm-settings']

/**
 * Shell for every authenticated route -- PolyWire's connection/traffic tools lead the sidebar
 * (that's the product being actively worked on day to day), Advisor's own per-database tools
 * collapse to a single grouped entry below it, since those four pages are now tabs on one
 * another rather than individual sidebar destinations (see AdvisorTabs).
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
        <div className={styles.railBrand}>
          <div className={styles.railMark}>PG</div>
          <div className={styles.railBrandText}>
            <span className={styles.railBrandTitle}>Polygres</span>
            <span className={styles.railBrandSub}>Advisor + Wire</span>
          </div>
        </div>

        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>PolyWire</div>
          <div className={styles.railGroup}>
            {WIRE_ITEMS.map(({ to, label, icon: Icon }) => (
              <NavLink key={to} to={to} className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
                <Icon size={17} strokeWidth={1.8} />
                {label}
              </NavLink>
            ))}
          </div>
        </div>

        <div className={styles.railDivider} />

        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>Advisor</div>
          <div className={styles.railGroup}>
            <NavLink
              to="/connections"
              className={() => `${styles.railItem} ${ADVISOR_ROUTES.some((r) => location.pathname.startsWith(r)) ? styles.railItemActive : ''}`}
            >
              <LayoutGrid size={17} strokeWidth={1.8} />
              Advisor
            </NavLink>
          </div>
        </div>

        <div className={styles.railSpacer} />

        <button onClick={handleLogout} className={styles.railItem} style={{ marginTop: 8 }}>
          <LogOut size={17} strokeWidth={1.8} />
          Sign out
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
