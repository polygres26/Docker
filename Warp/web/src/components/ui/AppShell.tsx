import { useEffect, useState } from 'react'
import { ChevronRight, LogOut, Menu, Monitor, Moon, Sun, X } from 'lucide-react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'
import { clearConnection, getStoredConnection, listNodes, type NodeInfo } from '../../api/client'
import logo from '../../assets/logo.png'
import styles from './AppShell.module.css'
import { NAV_GROUPS, sectionFor } from './nav'
import { RouteTabs } from './index'
import { useTheme } from './theme'

export { NAV_GROUPS }

function hostOf(baseUrl: string | undefined): string {
  if (!baseUrl) return ''
  try { return new URL(baseUrl).host } catch { return baseUrl }
}

/** Shell for every connected route: graphite rail (Workspace / Interfaces / Migration), top bar with
 * breadcrumb + theme toggle, optional section tab strip, and the page. Below 900px the rail is an off-canvas drawer. */
export default function AppShell({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate()
  const location = useLocation()
  const [open, setOpen] = useState(false)
  const [nodes, setNodes] = useState<NodeInfo[] | null>(null)
  const { pref, cycle } = useTheme()
  const conn = getStoredConnection()
  const section = sectionFor(location.pathname)
  const group = NAV_GROUPS.find((g) => section && g.items.includes(section))
  const tab = section?.tabs.find((t) => location.pathname === t.to || location.pathname.startsWith(t.to + '/'))
  const showTabCrumb = !!(tab && section && section.tabs.length > 1 && tab.label !== section.label)

  useEffect(() => { setOpen(false) }, [location.pathname])
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false) }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open])
  // Environment footer: real node heartbeats (absent on deployments that do not heartbeat).
  useEffect(() => { listNodes().then(setNodes).catch(() => setNodes(null)) }, [])

  function handleDisconnect() {
    clearConnection()
    navigate('/connect')
  }

  const stale = nodes?.filter((n) => n.status === 'stale').length ?? 0
  const ThemeIcon = pref === 'system' ? Monitor : pref === 'light' ? Sun : Moon
  const themeLabel = pref === 'system' ? 'Theme: follow system' : pref === 'light' ? 'Theme: light' : 'Theme: dark'

  return (
    <div className={styles.shell}>
      <a href="#main" className={styles.skip}>Skip to content</a>
      {open && <div className={styles.scrim} onClick={() => setOpen(false)} aria-hidden="true" />}
      <aside id="primary-nav" className={`${styles.rail} ${open ? styles.railOpen : ''}`}>
        <div className={styles.brand}>
          <span className={styles.logoTile}><img src={logo} alt="" className={styles.logo} /></span>
          <span className={styles.brandName}>Warp<small>Gateway console</small></span>
          <button type="button" className={styles.closeBtn} onClick={() => setOpen(false)} aria-label="Close navigation">
            <X size={18} />
          </button>
        </div>
        <nav aria-label="Primary">
          {NAV_GROUPS.map((g) => (
            <div key={g.label} role="group" aria-label={g.label}>
              <div className={styles.navLabel}>{g.label}</div>
              {g.items.map((item) => {
                const Icon = item.icon
                const active = section === item
                return (
                  <NavLink key={item.to} to={item.to} aria-current={active ? 'page' : undefined}
                    className={`${styles.navItem} ${active ? styles.navItemActive : ''}`}>
                    <Icon size={16} strokeWidth={1.8} aria-hidden="true" />
                    <span>{item.label}</span>
                  </NavLink>
                )
              })}
            </div>
          ))}
        </nav>
        <div className={styles.spacer} />
        <div className={styles.cluster}>
          <strong title={conn?.baseUrl}>{hostOf(conn?.baseUrl) || 'Not connected'}</strong>
          <span>
            {nodes && nodes.length > 0
              ? `${nodes.length} node${nodes.length === 1 ? '' : 's'} · ${stale > 0 ? `${stale} stale` : 'all reporting'}`
              : 'Warp admin endpoint'}
          </span>
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
            <nav className={styles.crumb} aria-label="Breadcrumb">
              {section && <section.icon size={16} strokeWidth={1.8} aria-hidden="true" />}
              {group && <><span className={styles.crumbDim}>{group.label}</span><ChevronRight size={13} aria-hidden="true" className={styles.crumbSep} /></>}
              <span aria-current={showTabCrumb ? undefined : 'page'}>{section?.label ?? 'Gateway'}</span>
              {showTabCrumb && tab && (
                <><ChevronRight size={13} aria-hidden="true" className={styles.crumbSep} /><span aria-current="page">{tab.label}</span></>
              )}
            </nav>
          </div>
          <div className={styles.topActions}>
            <button type="button" className={styles.iconBtn} onClick={cycle} aria-label={themeLabel} title={themeLabel}>
              <ThemeIcon size={16} strokeWidth={1.8} aria-hidden="true" />
            </button>
            <button type="button" className={styles.disconnect} onClick={handleDisconnect}>
              <LogOut size={15} strokeWidth={1.8} aria-hidden="true" />
              <span>Disconnect</span>
            </button>
          </div>
        </header>
        <main id="main" className={styles.page} tabIndex={-1}>
          {section && section.tabs.length > 1 && <RouteTabs items={section.tabs} label={`${section.label} pages`} />}
          {children}
        </main>
      </div>
    </div>
  )
}
