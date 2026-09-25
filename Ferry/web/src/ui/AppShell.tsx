import { useEffect, useState, type ComponentType, type ReactNode } from 'react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'
import { LogOut, Menu, X } from 'lucide-react'
import logo from '../assets/logo.png'
import { logout } from '../api/client'
import { Button } from './index'
import s from './AppShell.module.css'

export function NavGroup({ label, children }: { label: string; children: ReactNode }) {
  const id = `navgroup-${label.toLowerCase().replace(/\W+/g, '-')}`
  return (
    <div role="group" aria-labelledby={id}>
      <h2 id={id} className={s.navLabel}>{label}</h2>
      <ul className={s.navList}>{children}</ul>
    </div>
  )
}

export function NavItem({ to, icon: Icon, label, match }: {
  to: string
  icon: ComponentType<{ size?: number; strokeWidth?: number; 'aria-hidden'?: boolean }>
  label: string
  /** Extra path prefixes that should keep this item highlighted (tabbed sub-pages). */
  match?: string[]
}) {
  const { pathname } = useLocation()
  const extra = match?.some((p) => pathname === p || pathname.startsWith(p + '/'))
  return (
    <li>
      <NavLink
        to={to}
        end={!!match}
        className={({ isActive }) => `${s.navItem} ${isActive || extra ? s.navItemActive : ''}`}
        aria-current={extra ? 'page' : undefined}
      >
        <Icon size={16} strokeWidth={1.8} aria-hidden />
        {label}
      </NavLink>
    </li>
  )
}

/**
 * Sidebar + topbar frame for every authenticated route. Under 900px the sidebar becomes an
 * off-canvas drawer opened from the topbar menu button (closes on navigation / Escape / backdrop).
 */
export default function AppShell({ nav, section, children }: { nav: ReactNode; section?: string; children: ReactNode }) {
  const navigate = useNavigate()
  const { pathname } = useLocation()
  const [open, setOpen] = useState(false)

  useEffect(() => { setOpen(false) }, [pathname])
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false) }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open])

  async function handleLogout() {
    await logout()
    navigate('/login')
  }

  return (
    <div className={s.shell}>
      <aside id="app-rail" className={`${s.rail} ${open ? s.railOpen : ''}`} aria-label="Sidebar">
        <NavLink to="/dashboard" className={s.brand}>
          <span className={s.markTile}><img src={logo} alt="" /></span>
          <span className={s.brandText}>Sayonora<span className={s.brandSub}>Ferry</span></span>
        </NavLink>
        <nav className={s.nav} aria-label="Primary">{nav}</nav>
        <div className={s.railFoot}>
          <button type="button" className={s.navItem} onClick={handleLogout}>
            <LogOut size={16} strokeWidth={1.8} aria-hidden />
            Sign out
          </button>
        </div>
      </aside>
      <button type="button" className={s.backdrop} aria-label="Close menu" tabIndex={-1} onClick={() => setOpen(false)} />
      <div className={s.main}>
        <header className={s.topbar}>
          <div className={s.topLeft}>
            <Button variant="secondary" icon className={s.menuBtn} aria-label={open ? 'Close menu' : 'Open menu'} aria-expanded={open} aria-controls="app-rail" onClick={() => setOpen((v) => !v)}>
              {open ? <X size={16} aria-hidden /> : <Menu size={16} aria-hidden />}
            </Button>
            <div className={s.crumb}>
              <span>Sayonora Ferry</span>
              {section && <><span className={s.crumbSep} aria-hidden>/</span><span className={s.crumbCurrent}>{section}</span></>}
            </div>
          </div>
        </header>
        <main className={s.page}><div className={s.pageInner}>{children}</div></main>
      </div>
    </div>
  )
}
