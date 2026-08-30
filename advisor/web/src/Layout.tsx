import {
  Compass, Cpu, LayoutDashboard, LogOut, RefreshCw,
} from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { logout } from './api/client'
import logo from './assets/logo.png'
import styles from './Layout.module.css'

// Migration's own three pages (Connections, Reports, Sizing) live behind one sidebar entry --
// they're tabs on each other (see AdvisorTabs), not separate destinations, so the sidebar
// doesn't need to enumerate them. isActive covers every Migration route so this entry stays
// highlighted no matter which tab is open.
const MIGRATION_ROUTES = ['/connections', '/reports', '/sizing']

/**
 * Shell for every authenticated route. Two top-level groups: Migration -- Advisor's own
 * per-database assessment tools (Connections, Reports, Sizing, tabbed on one another, see
 * AdvisorTabs), and Configuration -- settings that aren't specific to any one tool. LLM lives
 * here since it drives Advisor's migration-report narrative generation.
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
          <img src={logo} alt="" className={styles.railMark} />
          <div className={styles.railBrandText}>
            <span className={styles.railBrandTitle}>Nexagres</span>
            <span className={styles.railBrandSub}>Advisor</span>
          </div>
        </div>

        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>Overview</div>
          <div className={styles.railGroup}>
            <NavLink to="/dashboard" className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
              <LayoutDashboard size={17} strokeWidth={1.8} />
              Dashboard
            </NavLink>
          </div>
        </div>

        <div className={styles.railDivider} />

        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>Migration</div>
          <div className={styles.railGroup}>
            <NavLink
              to="/connections"
              className={() => `${styles.railItem} ${MIGRATION_ROUTES.some((r) => location.pathname.startsWith(r)) ? styles.railItemActive : ''}`}
            >
              <Compass size={17} strokeWidth={1.8} />
              Advisor
            </NavLink>
          </div>
        </div>

        <div className={styles.railDivider} />

        {/* Deliberately named "Data Sync," not "Migration" -- the sidebar already has a
            "Migration" section above for Advisor's own per-database ASSESSMENT tools (Connections/
            Reports/Sizing). This is nexagres-migration's live data-movement progress report; the
            two products are easy to conflate by name, so this label is chosen to keep them
            visually distinct rather than reusing the word. */}
        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>Data Sync</div>
          <div className={styles.railGroup}>
            <NavLink to="/data-sync" className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
              <RefreshCw size={17} strokeWidth={1.8} />
              Progress
            </NavLink>
          </div>
        </div>

        <div className={styles.railDivider} />

        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>Configuration</div>
          <div className={styles.railGroup}>
            <NavLink to="/llm-settings" className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
              <Cpu size={17} strokeWidth={1.8} />
              LLM
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
          <div className={styles.crumb}>nexagres advisor</div>
        </div>
        <div className={styles.content}>{children}</div>
      </div>
    </div>
  )
}
