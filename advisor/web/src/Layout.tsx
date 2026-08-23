import {
  Activity, Compass, Cpu, KeyRound, ListOrdered, LogOut, Network, Route, Server, Shield, SlidersHorizontal, TableProperties,
} from 'lucide-react'
import { NavLink, useNavigate } from 'react-router-dom'
import { logout } from './api/client'
import styles from './Layout.module.css'

// Three groups instead of one flat list of seven: "how do I see what's happening" (Monitoring),
// "who's allowed to do what" (Security), and "where does traffic go and how fast" (Traffic) --
// the same three questions someone actually has when they open PolyWire's admin UI, in the order
// they usually ask them.
const WIRE_GROUPS = [
  {
    label: 'Monitoring',
    items: [
      { to: '/wire-metrics', label: 'Metrics', icon: Activity },
    ],
  },
  {
    label: 'Security',
    items: [
      { to: '/wire-firewall', label: 'SQL Firewall', icon: Shield },
      { to: '/wire-acl', label: 'ACL', icon: Network },
      { to: '/wire-oauth', label: 'OAuth', icon: KeyRound },
    ],
  },
  {
    label: 'Traffic',
    items: [
      { to: '/wire-backends', label: 'Backends', icon: Server },
      { to: '/wire-queues', label: 'Queues', icon: ListOrdered },
      { to: '/wire-data', label: 'Data explorer', icon: TableProperties },
      { to: '/wire-router', label: 'Router rules', icon: Route },
      { to: '/wire-qos', label: 'QoS', icon: SlidersHorizontal },
    ],
  },
]

// Migration's own three pages (Connections, Reports, Sizing) live behind one sidebar entry --
// they're tabs on each other (see AdvisorTabs), not separate destinations, so the sidebar
// doesn't need to enumerate them. isActive covers every Migration route so this entry stays
// highlighted no matter which tab is open.
const MIGRATION_ROUTES = ['/connections', '/reports', '/sizing']

/**
 * Shell for every authenticated route. Three top-level groups: PolyWire's connection/traffic
 * tools (the product being actively worked on day to day), Migration -- Advisor's own
 * per-database assessment tools (Connections, Reports, Sizing, tabbed on one another, see
 * AdvisorTabs), and Shared -- config that isn't specific to either product. LLM configuration
 * lives here, not under Migration, because the same Primary/Judge model config also drives
 * PolyWire's SQL translation and anything else that calls the LLM -- filing it under "Migration"
 * would have implied it was migration-only, which it isn't.
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
          {WIRE_GROUPS.map((group) => (
            <div key={group.label} className={styles.railSubGroup}>
              <div className={styles.railSubLabel}>{group.label}</div>
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

        <div className={styles.railSection}>
          <div className={styles.railSectionLabel}>Shared</div>
          <div className={styles.railGroup}>
            <NavLink to="/llm-settings" className={({ isActive }) => `${styles.railItem} ${isActive ? styles.railItemActive : ''}`}>
              <Cpu size={17} strokeWidth={1.8} />
              LLM configuration
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
