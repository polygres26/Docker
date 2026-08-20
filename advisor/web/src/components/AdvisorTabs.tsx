import { NavLink } from 'react-router-dom'
import styles from './AdvisorTabs.module.css'

const TABS = [
  { to: '/connections', label: 'Connections' },
  { to: '/reports', label: 'Reports' },
  { to: '/sizing', label: 'Sizing' },
  { to: '/llm-settings', label: 'LLM configuration' },
]

/**
 * Advisor's four sub-areas as tabs on one page, not four separate sidebar entries -- the sidebar
 * now leads with PolyWire (see Layout's group order) and collapses Advisor to a single "Advisor"
 * destination; this tab strip is what makes its own four pages still reachable from each other.
 * A route match on any TAB path renders this same strip via each page composing it at the top,
 * so switching tabs is a normal client-side navigation, not a nested router.
 */
export default function AdvisorTabs() {
  return (
    <div className={styles.tabs}>
      {TABS.map((t) => (
        <NavLink key={t.to} to={t.to} className={({ isActive }) => `${styles.tab} ${isActive ? styles.tabActive : ''}`}>
          {t.label}
        </NavLink>
      ))}
    </div>
  )
}
