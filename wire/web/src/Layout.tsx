import {
  Activity, Cpu, GitMerge, KeyRound, LayoutDashboard, ListOrdered, LogOut, Network, Route, Server, Shield, SlidersHorizontal, TableProperties, Waypoints,
} from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { clearConnection } from './api/client'
import logo from './assets/logo.png'
import styles from './Layout.module.css'

// Overview is its own single-item group (like versitygw's Admin Dashboard, always first) ahead
// of the three groups matching the questions someone actually has when they open Polywire's admin
// UI ("what's happening", "who's allowed to do what", "where does traffic go and how fast"), plus
// Configuration for settings that aren't specific to any one of those.
const GROUPS = [
  {
    label: 'Overview',
    items: [
      { to: '/dashboard', label: 'Dashboard', icon: LayoutDashboard },
    ],
  },
  {
    label: 'Monitoring',
    items: [
      { to: '/metrics', label: 'Metrics', icon: Activity },
      { to: '/topology', label: 'Topology', icon: Waypoints },
    ],
  },
  {
    label: 'Security',
    items: [
      { to: '/firewall', label: 'SQL Firewall', icon: Shield },
      { to: '/acl', label: 'ACL', icon: Network },
      { to: '/oauth', label: 'OAuth', icon: KeyRound },
    ],
  },
  {
    label: 'Traffic',
    items: [
      { to: '/backends', label: 'Backends', icon: Server },
      { to: '/queues', label: 'Queues', icon: ListOrdered },
      { to: '/data', label: 'Data explorer', icon: TableProperties },
      { to: '/router', label: 'Router rules', icon: Route },
      { to: '/qos', label: 'QoS', icon: SlidersHorizontal },
      { to: '/federation-plans', label: 'Federation Plans', icon: GitMerge },
    ],
  },
  {
    label: 'Configuration',
    items: [
      { to: '/llm-config', label: 'LLM', icon: Cpu },
    ],
  },
]

/** Shell for every connected route: labeled, grouped sidebar + topbar, matching dms/web's
 * Layout but scoped to Polywire alone -- there's no Migration/Advisor product sharing this rail. */
export default function Layout({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate()

  function handleDisconnect() {
    clearConnection()
    navigate('/connect')
  }

  return (
    <div className={styles.shell}>
      <nav className={styles.rail}>
        <div className={styles.railBrand}>
          <img src={logo} alt="" className={styles.railMark} />
          <div className={styles.railBrandText}>
            <span className={styles.railBrandTitle}>Polywire</span>
            <span className={styles.railBrandSub}>Admin console</span>
          </div>
        </div>

        {GROUPS.map((group) => (
          <div key={group.label} className={styles.railSection}>
            <div className={styles.railSectionLabel}>{group.label}</div>
            <div className={styles.railGroup}>
              {group.items.map(({ to, label, icon: Icon }) => (
                <NavLink key={to} to={to} className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
                  <Icon size={17} strokeWidth={1.8} />
                  {label}
                </NavLink>
              ))}
            </div>
          </div>
        ))}

        <div className={styles.railSpacer} />

        <button onClick={handleDisconnect} className={styles.railItem} style={{ marginTop: 8 }}>
          <LogOut size={17} strokeWidth={1.8} />
          Disconnect
        </button>
      </nav>
      <div className={styles.main}>
        <div className={styles.topbar}>
          <div className={styles.crumb}>polywire admin</div>
        </div>
        <div className={styles.content}>{children}</div>
      </div>
    </div>
  )
}
