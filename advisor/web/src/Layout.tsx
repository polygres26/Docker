import {
  Activity, Cpu, Database, FileUp, Gauge, KeyRound, LogOut, Network, Route, Server, Shield, SlidersHorizontal,
} from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { logout } from './api/client'
import styles from './Layout.module.css'

const ADVISOR_ITEMS = [
  { to: '/connections', label: 'Connections', icon: Database },
  { to: '/reports', label: 'Reports', icon: FileUp },
  { to: '/sizing', label: 'Sizing', icon: Gauge },
  { to: '/llm-settings', label: 'LLM configuration', icon: Cpu },
]

const WIRE_ITEMS = [
  { to: '/wire-metrics', label: 'Metrics', icon: Activity },
  { to: '/wire-firewall', label: 'SQL Firewall', icon: Shield },
  { to: '/wire-acl', label: 'ACL', icon: Network },
  { to: '/wire-backends', label: 'Backends', icon: Server },
  { to: '/wire-router', label: 'Router rules', icon: Route },
  { to: '/wire-qos', label: 'QoS', icon: SlidersHorizontal },
  { to: '/wire-oauth', label: 'OAuth', icon: KeyRound },
]

/**
 * Shell for every authenticated route -- a wide, labeled, grouped sidebar (Advisor's own
 * per-database tools, then PolyWire's connection/traffic management) plus a breadcrumb top bar.
 * Grouping replaced the earlier icon-only-with-tooltip rail once PolyWire's admin surface grew
 * past a handful of destinations -- hover-to-discover doesn't scale to 11 items, and the two
 * products' tools read as visibly separate concerns now, not one flat list.
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
          <div className={styles.railSectionLabel}>Advisor</div>
          <div className={styles.railGroup}>
            {ADVISOR_ITEMS.map(({ to, label, icon: Icon }) => (
              <NavLink key={to} to={to} className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
                <Icon size={17} strokeWidth={1.8} />
                {label}
              </NavLink>
            ))}
          </div>
        </div>

        <div className={styles.railDivider} />

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
