import { Database, LogOut } from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { logout } from './api/client'
import styles from './Layout.module.css'

/**
 * Shell for every authenticated route -- icon-only dark nav rail + a breadcrumb top bar, same
 * shape as Omnigate's AdminLayout (~/Projects/Omnigate/web/src/pages/admin/AdminLayout.tsx),
 * scoped down to the one nav destination Advisor has today (Connections). Structured the same
 * way regardless -- adding a second rail item later (e.g. a cross-connection reports view) is a
 * one-line addition to NAV_ITEMS, not a shell rewrite.
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
