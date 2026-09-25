import { useEffect, useState } from 'react'
import {
  Activity, Boxes, Cpu, GitMerge, KeyRound, Layers, LayoutDashboard, ListOrdered, LogOut,
  Menu, Network, Route, Shield, SlidersHorizontal, TableProperties, Waypoints, X,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'
import { clearConnection, getStoredConnection } from '../../api/client'
import logo from '../../assets/logo.png'
import styles from './AppShell.module.css'

interface NavEntry { to: string; label: string; icon: LucideIcon }
interface NavGroupDef { label: string; items: NavEntry[] }

/** Every pre-existing route is reachable from here. Groups follow the mock's Operate / Govern
 * split; Data holds the query/plan tooling and Configure the process-level settings. */
export const NAV_GROUPS: NavGroupDef[] = [
  {
    label: 'Operate',
    items: [
      { to: '/dashboard', label: 'Overview', icon: LayoutDashboard },
      { to: '/metrics', label: 'Traffic', icon: Activity },
      { to: '/topology', label: 'Topology', icon: Waypoints },
      { to: '/router', label: 'Router rules', icon: Route },
      { to: '/backend-sets', label: 'Backend sets', icon: Boxes },
      { to: '/queues', label: 'Queues', icon: ListOrdered },
    ],
  },
  {
    label: 'Govern',
    items: [
      { to: '/firewall', label: 'SQL firewall', icon: Shield },
      { to: '/acl', label: 'ACL', icon: Network },
      { to: '/oauth', label: 'OAuth', icon: KeyRound },
      { to: '/qos', label: 'QoS', icon: SlidersHorizontal },
    ],
  },
  {
    label: 'Data',
    items: [
      { to: '/data', label: 'Data explorer', icon: TableProperties },
      { to: '/federation-plans', label: 'Federation plans', icon: GitMerge },
      { to: '/rollups', label: 'Rollups', icon: Layers },
    ],
  },
  {
    label: 'Configure',
    items: [{ to: '/llm-config', label: 'LLM', icon: Cpu }],
  },
]

const ALL_ITEMS = NAV_GROUPS.flatMap((g) => g.items)

function hostOf(baseUrl: string | undefined): string {
  if (!baseUrl) return ''
  try { return new URL(baseUrl).host } catch { return baseUrl }
}

/** Shell for every connected route: graphite rail with grouped navigation, a 58px topbar, and a
 * page area. Below 900px the rail becomes an off-canvas drawer opened from the topbar. */
export default function AppShell({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate()
  const location = useLocation()
  const [open, setOpen] = useState(false)
  const conn = getStoredConnection()
  const current = ALL_ITEMS.find((i) => location.pathname === i.to || location.pathname.startsWith(i.to + '/'))

  // Close the drawer on navigation and on Escape.
  useEffect(() => { setOpen(false) }, [location.pathname])
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false) }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open])

  function handleDisconnect() {
    clearConnection()
    navigate('/connect')
  }

  return (
    <div className={styles.shell}>
      <a href="#main" className={styles.skip}>Skip to content</a>
      {open && <div className={styles.scrim} onClick={() => setOpen(false)} aria-hidden="true" />}
      <aside id="primary-nav" className={`${styles.rail} ${open ? styles.railOpen : ''}`}>
        <div className={styles.brand}>
          <span className={styles.logoTile}><img src={logo} alt="" className={styles.logo} /></span>
          <span className={styles.brandName}>Sayonora</span>
          <button type="button" className={styles.closeBtn} onClick={() => setOpen(false)} aria-label="Close navigation">
            <X size={18} />
          </button>
        </div>
        <nav aria-label="Primary">
          {NAV_GROUPS.map((group) => (
            <div key={group.label} role="group" aria-label={group.label}>
              <div className={styles.navLabel}>{group.label}</div>
              {group.items.map(({ to, label, icon: Icon }) => (
                <NavLink key={to} to={to} className={({ isActive }) => `${styles.navItem} ${isActive ? styles.navItemActive : ''}`}>
                  <Icon size={16} strokeWidth={1.8} aria-hidden="true" />
                  <span>{label}</span>
                </NavLink>
              ))}
            </div>
          ))}
        </nav>
        <div className={styles.spacer} />
        <div className={styles.cluster}>
          <strong title={conn?.baseUrl}>{hostOf(conn?.baseUrl) || 'Not connected'}</strong>
          <span>Warp admin endpoint</span>
        </div>
      </aside>

      <div className={styles.main}>
        <header className={styles.topbar}>
          <div className={styles.topLeft}>
            <button type="button" className={styles.menuBtn} onClick={() => setOpen(true)}
              aria-label="Open navigation" aria-expanded={open} aria-controls="primary-nav">
              <Menu size={18} />
            </button>
            <span className={styles.mobileBrand}><img src={logo} alt="" className={styles.logoSm} /></span>
            <span className={styles.crumb}>
              {current && <current.icon size={16} strokeWidth={1.8} aria-hidden="true" />}
              {current?.label ?? 'Gateway'}
            </span>
          </div>
          <button type="button" className={styles.disconnect} onClick={handleDisconnect}>
            <LogOut size={15} strokeWidth={1.8} aria-hidden="true" />
            <span>Disconnect</span>
          </button>
        </header>
        <main id="main" className={styles.page} tabIndex={-1}>{children}</main>
      </div>
    </div>
  )
}
