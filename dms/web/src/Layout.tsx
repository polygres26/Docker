import {
  Compass, Cpu, LayoutDashboard, LogOut, RefreshCw,
} from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { logout } from './api/client'
import logo from './assets/logo.png'
import styles from './Layout.module.css'

// Migration Advisor's own three pages (Connections, Reports, Sizing) live behind one sidebar
// entry -- they're tabs on each other (see DmsTabs), not separate destinations, so the sidebar
// doesn't need to enumerate them. isActive covers every route so this entry stays highlighted no
// matter which tab is open.
const MIGRATION_ADVISOR_ROUTES = ['/connections', '/reports', '/sizing']

/**
 * Shell for every authenticated route. Nexagres DMS's two halves each get their own sidebar
 * section: Migration Advisor -- per-database assessment tools (Connections, Reports, Sizing,
 * tabbed on one another, see DmsTabs) -- and Migration Service -- nexagres-migration's real
 * data-movement jobs and their live progress (the Data Sync page). Plus Configuration, settings
 * that aren't specific to either half; LLM lives there since it drives Migration Advisor's
 * migration-report narrative generation.
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
            <span className={styles.railBrandSub}>DMS</span>
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
          <div className={styles.railSectionLabel}>Migration Advisor</div>
          <div className={styles.railGroup}>
            <NavLink
              to="/connections"
              className={() => `${styles.railItem} ${MIGRATION_ADVISOR_ROUTES.some((r) => location.pathname.startsWith(r)) ? styles.railItemActive : ''}`}
            >
              <Compass size={17} strokeWidth={1.8} />
              Assessment
            </NavLink>
          </div>
        </div>

        <div className={styles.railDivider} />

        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>Migration Service</div>
          <div className={styles.railGroup}>
            <NavLink to="/data-sync" className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
              <RefreshCw size={17} strokeWidth={1.8} />
              Data Sync
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
          <div className={styles.crumb}>nexagres dms</div>
        </div>
        <div className={styles.content}>{children}</div>
      </div>
    </div>
  )
}
